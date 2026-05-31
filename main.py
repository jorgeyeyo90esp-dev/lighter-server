import asyncio
import json
import os
import io
import csv
import time
import logging
from datetime import datetime, timezone, timedelta
from aiohttp import web, ClientSession, WSMsgType

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger(__name__)

trades = {}
funding = {}
positions = {}
market_map = {}
connected = False
last_update = 0
initial_load_done = False
last_incremental = 0

TOKEN = os.environ.get('LIGHTER_TOKEN', '')
BASE = 'https://mainnet.zklighter.elliot.ai'
BASE_WS = 'wss://mainnet.zklighter.elliot.ai/stream'
GENESIS_MS = 1737072000000

def get_account():
    try: return TOKEN.split(':')[1]
    except: return None

def hdrs():
    return {'Authorization': TOKEN}

def to_ms(dt):
    return int(dt.timestamp() * 1000)

def from_ms(ms):
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc)

def today_start_ms():
    now = datetime.now(timezone.utc)
    return to_ms(now.replace(hour=0, minute=0, second=0, microsecond=0))

async def load_markets(session):
    global market_map
    try:
        async with session.get(BASE + '/api/v1/orderBookDetails') as r:
            if r.status == 200:
                for m in (await r.json()).get('order_book_details', []):
                    market_map[str(m['market_id'])] = m['symbol']
                log.info(f"Markets: {len(market_map)}")
    except Exception as e:
        log.error(f"Markets: {e}")

def sym(mid):
    return market_map.get(str(mid), f'market_{mid}')

def parse_trade_csv(text):
    result = {}
    try:
        for row in csv.DictReader(io.StringIO(text.strip())):
            market = row.get('Market', '?')
            side_raw = row.get('Side', '').lower()
            is_open = 'open' in side_raw
            is_long = 'long' in side_raw
            pnl_raw = row.get('Closed PnL', '-')
            pnl = None if pnl_raw in ('-', '', 'null') else float(pnl_raw)
            date_str = row.get('Date', '')
            try:
                ts = to_ms(datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc))
            except:
                ts = int(time.time() * 1000)
            price = float(row.get('Price', 0) or 0)
            size = float(row.get('Size', 0) or 0)
            fee = float(row.get('Fee', 0) or 0)
            tid = f"{date_str}_{market}_{side_raw}_{price}_{size}".replace(' ', '_')
            result[tid] = {
                'id': tid, 'symbol': market,
                'side': 'long' if is_long else 'short',
                'tradeType': 'open' if is_open else 'close',
                'price': price, 'size': size, 'pnl': pnl,
                'fee': fee, 'ts': ts, 'source': 'export'
            }
    except Exception as e:
        log.error(f"parse_trade_csv: {e}")
    return result

def parse_funding_csv(text):
    result = {}
    try:
        for i, row in enumerate(csv.DictReader(io.StringIO(text.strip()))):
            market = row.get('Market', '?')
            date_str = row.get('Date', '')
            payment = float(row.get('Payment', 0) or 0)
            try:
                ts = to_ms(datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc))
            except:
                ts = int(time.time() * 1000)
            fid = f"f_{date_str}_{market}_{i}".replace(' ', '_')
            result[fid] = {
                'id': fid, 'symbol': market,
                'side': row.get('Side', 'long').lower(),
                'payment': payment,
                'rate': row.get('Rate', ''),
                'ts': ts
            }
    except Exception as e:
        log.error(f"parse_funding_csv: {e}")
    return result

async def export_call(session, account, start_ms, end_ms, etype):
    url = f"{BASE}/api/v1/export?account_index={account}&type={etype}&start_timestamp={start_ms}&end_timestamp={end_ms}"
    try:
        async with session.get(url, headers=hdrs()) as r:
            if r.status != 200:
                return None
            data = await r.json()
            data_url = data.get('data_url') or data.get('url')
            if not data_url:
                return None
        async with session.get(data_url) as r:
            if r.status != 200:
                return None
            return await r.text()
    except Exception as e:
        log.error(f"export_call {etype}: {e}")
        return None

async def historical_load(session, account):
    global initial_load_done
    log.info("=== HISTORICAL LOAD START ===")
    now = datetime.now(timezone.utc)
    start = from_ms(GENESIS_MS).replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    chunks = []
    cur = start
    while cur < now:
        nxt = (cur.replace(day=28) + timedelta(days=4)).replace(day=1)
        chunks.append((to_ms(cur), to_ms(min(nxt, now))))
        cur = nxt
    log.info(f"Loading {len(chunks)} monthly chunks")
    for i, (s, e) in enumerate(chunks):
        label = from_ms(s).strftime('%Y-%m')
        text = await export_call(session, account, s, e, 'trade')
        if text:
            chunk = parse_trade_csv(text)
            trades.update(chunk)
            log.info(f"Chunk {i+1}/{len(chunks)} {label}: +{len(chunk)} trades (total {len(trades)})")
        await asyncio.sleep(0.3)
        text = await export_call(session, account, s, e, 'funding')
        if text:
            chunk = parse_funding_csv(text)
            funding.update(chunk)
        await asyncio.sleep(0.3)
    wp = sum(1 for t in trades.values() if t.get('pnl') is not None)
    ft = round(sum(f['payment'] for f in funding.values()), 4)
    log.info(f"=== HISTORICAL LOAD DONE: {len(trades)} trades ({wp} with PnL), {len(funding)} funding, funding total={ft} ===")
    initial_load_done = True

async def incremental_update(session, account):
    global last_incremental
    now_ms = int(time.time() * 1000)
    ts = today_start_ms()
    log.info(f"Incremental update from {from_ms(ts).strftime('%Y-%m-%d')}")
    text = await export_call(session, account, ts, now_ms, 'trade')
    if text:
        new = parse_trade_csv(text)
        before = len(trades)
        trades.update(new)
        log.info(f"Incremental trades: +{len(trades)-before} new (total {len(trades)})")
    await asyncio.sleep(0.3)
    text = await export_call(session, account, ts, now_ms, 'funding')
    if text:
        new = parse_funding_csv(text)
        funding.update(new)
    await load_positions(session, account)
    last_incremental = now_ms
    log.info("Incremental done")

async def load_positions(session, account):
    global positions
    try:
        async with session.get(f"{BASE}/api/v1/account?by=index&value={account}", headers=hdrs()) as r:
            if r.status == 200:
                for mid, pos in ((await r.json()).get('positions') or {}).items():
                    positions[str(mid)] = {
                        'market_id': mid, 'symbol': sym(mid),
                        'side': 'long' if int(pos.get('sign', 1)) > 0 else 'short',
                        'size': float(pos.get('position', 0)),
                        'avg_entry': float(pos.get('avg_entry_price', 0)),
                        'unrealized_pnl': float(pos.get('unrealized_pnl', 0)),
                        'realized_pnl': float(pos.get('realized_pnl', 0)),
                        'liquidation_price': float(pos.get('liquidation_price', 0)),
                    }
    except Exception as e:
        log.error(f"positions: {e}")

def process_ws_trade(t, account):
    try:
        tid = str(t.get('trade_id') or t.get('id', ''))
        if not tid or tid in trades: return
        is_ask = str(t.get('ask_account_id', '')) == str(account)
        trades[tid] = {
            'id': tid, 'symbol': sym(str(t.get('market_id', ''))),
            'side': 'short' if is_ask else 'long',
            'tradeType': 'unknown',
            'price': float(t.get('price', 0)),
            'size': float(t.get('size', 0)),
            'pnl': None,
            'fee': float(t.get('taker_fee') or t.get('maker_fee') or 0),
            'ts': t.get('timestamp') or int(time.time() * 1000),
            'source': 'ws'
        }
        global last_update
        last_update = int(time.time() * 1000)
        log.info(f"WS trade: {trades[tid]['symbol']}")
    except Exception as e:
        log.debug(f"ws_trade: {e}")

def process_ws_position(p):
    try:
        mid = str(p.get('market_id', ''))
        if not mid: return
        positions[mid] = {
            'market_id': mid, 'symbol': sym(mid),
            'side': 'long' if int(p.get('sign', 1)) > 0 else 'short',
            'size': float(p.get('position', 0)),
            'avg_entry': float(p.get('avg_entry_price', 0)),
            'unrealized_pnl': float(p.get('unrealized_pnl', 0)),
            'realized_pnl': float(p.get('realized_pnl', 0)),
            'liquidation_price': float(p.get('liquidation_price', 0)),
        }
        global last_update
        last_update = int(time.time() * 1000)
    except Exception as e:
        log.debug(f"ws_pos: {e}")

async def ws_listener():
    global connected, last_update
    account = get_account()
    if not TOKEN or not account:
        log.error("No LIGHTER_TOKEN")
        return
    async with ClientSession() as session:
        await load_markets(session)
        await load_positions(session, account)
        await historical_load(session, account)
        async def scheduler():
            while True:
                await asyncio.sleep(900)
                await incremental_update(session, account)
        asyncio.ensure_future(scheduler())
        while True:
            try:
                async with session.ws_connect(BASE_WS, heartbeat=60) as ws:
                    connected = True
                    await ws.send_json({"type": "subscribe", "channel": f"account_all_trades/{account}", "auth": TOKEN})
                    await ws.send_json({"type": "subscribe", "channel": f"account_all_positions/{account}", "auth": TOKEN})
                    log.info(f"WS connected account {account}")
                    async for msg in ws:
                        if msg.type == WSMsgType.TEXT:
                            try:
                                d = json.loads(msg.data)
                                mt = d.get('type', '')
                                if 'trade' in mt.lower():
                                    td = d.get('trade') or d.get('trades') or d.get('data')
                                    for t in ([td] if isinstance(td, dict) else (td or [])):
                                        process_ws_trade(t, account)
                                elif 'position' in mt.lower():
                                    pd = d.get('position') or d.get('positions') or d.get('data')
                                    for p in ([pd] if isinstance(pd, dict) else (pd or [])):
                                        process_ws_position(p)
                                last_update = int(time.time() * 1000)
                            except: pass
                        elif msg.type in (WSMsgType.CLOSED, WSMsgType.ERROR):
                            break
            except Exception as e:
                log.error(f"WS: {e}")
            connected = False
            await asyncio.sleep(5)

def cors(r):
    r.headers['Access-Control-Allow-Origin'] = '*'
    return r

async def h_root(req):
    return cors(web.json_response({'ok': True, 'loading': not initial_load_done}))

async def h_status(req):
    tp = round(sum(t['pnl'] for t in trades.values() if t.get('pnl') is not None), 4)
    ft = round(sum(f['payment'] for f in funding.values()), 4)
    return cors(web.json_response({
        'ok': True, 'connected': connected,
        'account': get_account(),
        'initial_load_done': initial_load_done,
        'trades': len(trades), 'funding': len(funding),
        'positions': len(positions),
        'trade_pnl': tp, 'funding_total': ft,
        'total_pnl': round(tp + ft, 4),
        'last_incremental': last_incremental,
        'last_update': last_update,
        'ts': int(time.time() * 1000)
    }))

async def h_trades(req):
    limit = int(req.rel_url.query.get('limit', 20000))
    sym_f = req.rel_url.query.get('symbol', '').lower()
    all_t = sorted(trades.values(), key=lambda t: int(t.get('ts', 0) or 0), reverse=True)
    if sym_f:
        all_t = [t for t in all_t if sym_f in (t.get('symbol') or '').lower()]
    return cors(web.json_response({
        'trades': all_t[:limit],
        'total': len(all_t),
        'loading': not initial_load_done
    }))

async def h_funding(req):
    all_f = sorted(funding.values(), key=lambda f: int(f.get('ts', 0) or 0), reverse=True)
    return cors(web.json_response({
        'funding': all_f,
        'total': round(sum(f['payment'] for f in all_f), 4),
        'count': len(all_f)
    }))

async def h_positions(req):
    return cors(web.json_response({'positions': list(positions.values())}))

async def h_summary(req):
    ts = today_start_ms()
    closes = [t for t in trades.values() if t.get('tradeType') == 'close' and t.get('pnl') is not None]
    pnls = [t['pnl'] for t in closes]
    tp = round(sum(pnls), 4)
    ft = round(sum(f['payment'] for f in funding.values()), 4)
    wins = sum(1 for p in pnls if p > 0)
    losses = sum(1 for p in pnls if p < 0)
    wr = round(wins / len(pnls) * 100, 1) if pnls else 0
    today_c = [t for t in closes if int(t.get('ts', 0) or 0) >= ts]
    today_pnl = round(sum(t['pnl'] for t in today_c), 4)
    today_f = round(sum(f['payment'] for f in funding.values() if int(f.get('ts', 0) or 0) >= ts), 4)
    by_sym = {}
    for t in closes:
        s = t.get('symbol', '?')
        if s not in by_sym:
            by_sym[s] = {'symbol': s, 'trades': 0, 'pnl': 0.0, 'wins': 0, 'losses': 0, 'best': None, 'worst': None}
        m = by_sym[s]
        m['trades'] += 1; m['pnl'] += t['pnl']
        if t['pnl'] > 0: m['wins'] += 1
        elif t['pnl'] < 0: m['losses'] += 1
        if m['best'] is None or t['pnl'] > m['best']: m['best'] = t['pnl']
        if m['worst'] is None or t['pnl'] < m['worst']: m['worst'] = t['pnl']
    for s in by_sym:
        by_sym[s]['pnl'] = round(by_sym[s]['pnl'], 4)
        if by_sym[s]['best']: by_sym[s]['best'] = round(by_sym[s]['best'], 4)
        if by_sym[s]['worst']: by_sym[s]['worst'] = round(by_sym[s]['worst'], 4)
    return cors(web.json_response({
        'total_pnl': round(tp + ft, 4),
        'trade_pnl': tp, 'funding_total': ft,
        'today_pnl': round(today_pnl + today_f, 4),
        'today_trade_pnl': today_pnl, 'today_funding': today_f,
        'total_trades': len(trades), 'closed_trades': len(closes),
        'today_trades': len(today_c),
        'wins': wins, 'losses': losses, 'win_rate': wr,
        'by_symbol': list(by_sym.values()),
        'positions': list(positions.values()),
        'connected': connected,
        'initial_load_done': initial_load_done,
        'last_update': last_update
    }))

async def h_options(req):
    return web.Response(headers={
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'GET, OPTIONS',
        'Access-Control-Allow-Headers': '*'
    })

async def on_start(app):
    app['task'] = asyncio.ensure_future(ws_listener())

async def on_stop(app):
    app['task'].cancel()
    try: await app['task']
    except asyncio.CancelledError: pass

def create_app():
    app = web.Application()
    app.router.add_get('/', h_root)
    app.router.add_get('/status', h_status)
    app.router.add_get('/trades', h_trades)
    app.router.add_get('/funding', h_funding)
    app.router.add_get('/positions', h_positions)
    app.router.add_get('/summary', h_summary)
    app.router.add_options('/{p:.*}', h_options)
    app.on_startup.append(on_start)
    app.on_cleanup.append(on_stop)
    return app

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 10000))
    log.info(f"Starting on port {port}")
    web.run_app(create_app(), port=port)
