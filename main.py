import asyncio, json, os, time, logging, io, csv
from datetime import datetime, timezone, timedelta
from aiohttp import web, ClientSession, WSMsgType

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger(__name__)

TOKEN  = os.environ.get('LIGHTER_TOKEN', '')
TOKEN2 = os.environ.get('LIGHTER_TOKEN_2', '')
BASE   = 'https://mainnet.zklighter.elliot.ai'
BASE_WS= 'wss://mainnet.zklighter.elliot.ai/stream'
GENESIS_MS  = 1737072000000
GENESIS2_MS = 1788220800000

trades={};funding={};positions={}
trades2={};funding2={};positions2={}
market_map={}
initial_load_done=False;initial_load_done2=False
movimientos_data={'account1':[],'account2':[],'reserva_pct':25}

def get_account():
    try: return TOKEN.split(':')[1]
    except: return None
def get_account2():
    try: return TOKEN2.split(':')[1]
    except: return None
def hdrs():  return {'Authorization': TOKEN}
def hdrs2(): return {'Authorization': TOKEN2}
def to_ms(dt): return int(dt.timestamp()*1000)
def from_ms(ms): return datetime.fromtimestamp(ms/1000,tz=timezone.utc)
def today_start_ms():
    n=datetime.now(timezone.utc)
    return to_ms(n.replace(hour=0,minute=0,second=0,microsecond=0))
def sym(mid): return market_map.get(str(mid),f'market_{mid}')
def cors(r):
    r.headers['Access-Control-Allow-Origin']='*'
    r.headers['Access-Control-Allow-Methods']='GET,POST,OPTIONS'
    r.headers['Access-Control-Allow-Headers']='Content-Type'
    return r

async def load_markets(session):
    try:
        async with session.get(f"{BASE}/api/v1/orderBookDetails") as r:
            if r.status==200:
                for m in ((await r.json()).get('order_books') or []):
                    mid=str(m.get('market_id',''))
                    base=(m.get('base_asset') or {}).get('symbol','')
                    if base: market_map[mid]=base
                log.info(f"Markets: {len(market_map)}")
    except Exception as e: log.error(f"markets: {e}")

def parse_csv(text):
    result={}
    try:
        for row in csv.DictReader(io.StringIO(text)):
            ds=row.get('Date','').strip();market=row.get('Market','').strip()
            side=row.get('Side','').strip();price=float(row.get('Price',0) or 0)
            size=float(row.get('Size',0) or 0)
            pnl_r=row.get('Closed PnL','-').strip()
            fee_r=row.get('Fee','0').strip()
            pnl=float(pnl_r) if pnl_r not in('-','','None') else None
            fee=float(fee_r) if fee_r not in('-','','None') else 0.0
            tv=row.get('Trade Value','').strip()
            tid=f"{ds}_{market}_{side}_{price}_{size}_{tv}".replace(' ','_')
            ts=int(datetime.strptime(ds,'%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc).timestamp()*1000) if ds else 0
            result[tid]={'id':tid,'symbol':market,'side':'long' if 'long' in side.lower() else 'short',
                'tradeType':'close' if 'close' in side.lower() else 'open',
                'price':price,'size':size,'pnl':pnl,'fee':fee,'ts':ts}
    except Exception as e: log.error(f"parse_csv: {e}")
    return result

async def export_call(session,account,s,e,h):
    url=f"{BASE}/api/v1/export?account_index={account}&type=trade&start_timestamp={s}&end_timestamp={e}"
    try:
        async with session.get(url,headers=h) as r:
            if r.status!=200: return None
            data=await r.json()
            du=data.get('data_url') or data.get('url')
            if not du: return None
            async with session.get(du) as r2:
                if r2.status==200: return await r2.text()
    except Exception as e2: log.debug(f"export: {e2}")
    return None

async def load_funding_data(session,account,store,h,start_ts=None):
    now_ms=int(time.time()*1000);start=start_ts or GENESIS_MS;cursor=None;total=0
    while True:
        url=f"{BASE}/api/v1/positionFunding?account_index={account}&limit=100&start_timestamp={start}&end_timestamp={now_ms}"
        if cursor: url+=f"&cursor={cursor}"
        try:
            async with session.get(url,headers=h) as r:
                if r.status!=200: break
                data=await r.json();items=data.get('position_fundings',[])
                if not items: break
                for item in items:
                    fid=str(item.get('funding_id',''));mid=str(item.get('market_id',''))
                    pay=float(item.get('change',0));ts_v=item.get('timestamp',now_ms)
                    if fid: store[fid]={'id':fid,'symbol':sym(mid),'payment':pay,'ts':ts_v};total+=1
                cursor=data.get('next_cursor')
                if not cursor or len(items)<100: break
                await asyncio.sleep(0.15)
        except Exception as e: log.error(f"funding: {e}"); break
    ft=round(sum(f['payment'] for f in store.values()),4)
    log.info(f"Funding loaded: {total} payments, total={ft}")

async def historical_load(session,account,store,h,genesis):
    now=datetime.now(timezone.utc)
    gen=from_ms(genesis).replace(day=1,hour=0,minute=0,second=0,microsecond=0)
    cur=gen;chunks=[]
    while cur<now:
        nxt=cur+timedelta(days=7)
        chunks.append((to_ms(cur),to_ms(min(nxt,now))))
        cur=nxt
    log.info(f"Loading {len(chunks)} weekly chunks for account {account}")
    for i,(s,e) in enumerate(chunks):
        label=from_ms(s).strftime('%Y-%m-%d')
        text=await export_call(session,account,s,e,h)
        if text:
            chunk=parse_csv(text);before=len(store);store.update(chunk);added=len(store)-before
            if added>0: log.info(f"Chunk {i+1}/{len(chunks)} {label}: +{added} (total {len(store)})")
        await asyncio.sleep(0.2)
    wp=sum(1 for t in store.values() if t.get('pnl') is not None)
    log.info(f"=== DONE account {account}: {len(store)} trades ({wp} with PnL) ===")

def build_summary(st,sf,sp,done):
    now=int(time.time()*1000);ts=today_start_ms()
    closes=[t for t in st.values() if t.get('tradeType')=='close' and t.get('pnl') is not None]
    pnls=[t['pnl'] for t in closes];tp=round(sum(pnls),4)
    ft=round(sum(f['payment'] for f in sf.values()),4)
    fee_t=round(sum(float(t.get('fee') or 0) for t in st.values()),4)
    wins=sum(1 for p in pnls if p>0);losses=sum(1 for p in pnls if p<0)
    wr=round(wins/len(pnls)*100,1) if pnls else 0
    today_pnl=round(sum(t['pnl'] for t in closes if int(t.get('ts',0) or 0)>=ts),4)
    p7=round(sum(t['pnl'] for t in closes if int(t.get('ts',0) or 0)>=now-7*86400000),4)
    p30=round(sum(t['pnl'] for t in closes if int(t.get('ts',0) or 0)>=now-30*86400000),4)
    by_sym={}
    for t in closes:
        s=t.get('symbol','?')
        if s not in by_sym: by_sym[s]={'symbol':s,'trades':0,'pnl':0.0,'wins':0,'losses':0,'best':None,'worst':None,'funding':0,'total_pnl':0}
        m=by_sym[s];m['trades']+=1;m['pnl']+=t['pnl']
        if t['pnl']>0: m['wins']+=1
        else: m['losses']+=1
        if m['best'] is None or t['pnl']>m['best']: m['best']=t['pnl']
        if m['worst'] is None or t['pnl']<m['worst']: m['worst']=t['pnl']
    for f in sf.values():
        s=f.get('symbol','?')
        if s in by_sym: by_sym[s]['funding']=round(by_sym[s].get('funding',0)+f['payment'],4)
    for s in by_sym: by_sym[s]['pnl']=round(by_sym[s]['pnl'],4);by_sym[s]['total_pnl']=round(by_sym[s]['pnl']+by_sym[s].get('funding',0),4)
    return {'total_pnl':round(tp+ft,4),'trade_pnl':tp,'funding_total':ft,'fee_total':fee_t,
            'today_pnl':today_pnl,'p7':p7,'p30':p30,'total_trades':len(st),'closed_trades':len(closes),
            'wins':wins,'losses':losses,'win_rate':wr,'by_symbol':list(by_sym.values()),
            'positions':list(sp.values()),'initial_load_done':done,'last_update':now}

async def h_root(req):    return cors(web.json_response({'ok':True,'loading':not initial_load_done}))
async def h_summary(req): return cors(web.json_response(build_summary(trades,funding,positions,initial_load_done)))
async def h_summary2(req):return cors(web.json_response(build_summary(trades2,funding2,positions2,initial_load_done2)))
async def h_trades(req):
    limit=int(req.rel_url.query.get('limit',50000))
    all_t=sorted(trades.values(),key=lambda t:int(t.get('ts',0) or 0),reverse=True)
    return cors(web.json_response({'trades':all_t[:limit],'total':len(all_t),'loading':not initial_load_done}))
async def h_trades2(req):
    limit=int(req.rel_url.query.get('limit',50000))
    all_t=sorted(trades2.values(),key=lambda t:int(t.get('ts',0) or 0),reverse=True)
    return cors(web.json_response({'trades':all_t[:limit],'total':len(all_t),'loading':not initial_load_done2}))
async def h_funding(req):
    all_f=sorted(funding.values(),key=lambda f:int(f.get('ts',0) or 0),reverse=True)
    return cors(web.json_response({'funding':all_f,'total':round(sum(f['payment'] for f in funding.values()),4)}))
async def h_funding2(req):
    all_f=sorted(funding2.values(),key=lambda f:int(f.get('ts',0) or 0),reverse=True)
    return cors(web.json_response({'funding':all_f,'total':round(sum(f['payment'] for f in funding2.values()),4)}))
async def h_movimientos(req):    return cors(web.json_response(movimientos_data))
async def h_save_movimientos(req):
    global movimientos_data
    try: movimientos_data=await req.json(); return cors(web.json_response({'ok':True}))
    except Exception as e: return cors(web.json_response({'ok':False,'error':str(e)}))
async def h_options(req): return cors(web.Response(status=200))

async def run_account1():
    global initial_load_done
    account=get_account()
    if not TOKEN or not account: log.error("No LIGHTER_TOKEN"); return
    async with ClientSession() as session:
        await load_markets(session)
        await historical_load(session,account,trades,hdrs(),GENESIS_MS)
        await load_funding_data(session,account,funding,hdrs())
        initial_load_done=True
        async def scheduler():
            while True:
                await asyncio.sleep(900)
                try:
                    now_ms=int(time.time()*1000);now_dt=datetime.now(timezone.utc)
                    prev_m=(now_dt.replace(day=1)-timedelta(days=1)).replace(day=1,hour=0,minute=0,second=0,microsecond=0)
                    ts=to_ms(prev_m)
                    text=await export_call(session,account,ts,now_ms,hdrs())
                    if text:
                        new=parse_csv(text);before=len(trades);trades.update(new)
                        log.info(f"Incremental A1: +{len(trades)-before} trades")
                    await load_funding_data(session,account,funding,hdrs(),start_ts=ts)
                except Exception as e: log.error(f"Incr A1: {e}")
        asyncio.ensure_future(scheduler())
        while True:
            try:
                async with session.ws_connect(BASE_WS,heartbeat=60) as ws:
                    await ws.send_json({"type":"subscribe","channel":f"account_all_trades/{account}","auth":TOKEN})
                    log.info(f"WS connected A1 {account}")
                    async for msg in ws:
                        if msg.type in(WSMsgType.CLOSED,WSMsgType.ERROR): break
            except: pass
            await asyncio.sleep(5)

async def run_account2():
    global initial_load_done2
    account=get_account2()
    if not TOKEN2 or not account: log.info("No LIGHTER_TOKEN_2"); return
    async with ClientSession() as session:
        await historical_load(session,account,trades2,hdrs2(),GENESIS2_MS)
        await load_funding_data(session,account,funding2,hdrs2(),start_ts=GENESIS2_MS)
        initial_load_done2=True
        while True:
            await asyncio.sleep(900)
            try:
                now_ms=int(time.time()*1000);now_dt=datetime.now(timezone.utc)
                prev_m=(now_dt.replace(day=1)-timedelta(days=1)).replace(day=1,hour=0,minute=0,second=0,microsecond=0)
                ts=to_ms(prev_m)
                text=await export_call(session,account,ts,now_ms,hdrs2())
                if text:
                    new=parse_csv(text);before=len(trades2);trades2.update(new)
                    log.info(f"Incremental A2: +{len(trades2)-before} trades")
                await load_funding_data(session,account,funding2,hdrs2(),start_ts=ts)
            except Exception as e: log.error(f"Incr A2: {e}")

async def on_start(app):
    app['t1']=asyncio.ensure_future(run_account1())
    if TOKEN2: app['t2']=asyncio.ensure_future(run_account2())

def create_app():
    app=web.Application()
    app.router.add_get('/',h_root)
    app.router.add_get('/summary',h_summary)
    app.router.add_get('/summary2',h_summary2)
    app.router.add_get('/trades',h_trades)
    app.router.add_get('/trades2',h_trades2)
    app.router.add_get('/funding',h_funding)
    app.router.add_get('/funding2',h_funding2)
    app.router.add_get('/movimientos',h_movimientos)
    app.router.add_post('/movimientos',h_save_movimientos)
    app.router.add_route('OPTIONS','/{path_info:.*}',h_options)
    app.on_startup.append(on_start)
    return app

if __name__=='__main__':
    port=int(os.environ.get('PORT',10000))
    log.info(f"Starting on port {port}")
    web.run_app(create_app(),port=port)
