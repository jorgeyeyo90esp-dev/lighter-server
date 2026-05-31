import asyncio
import json
import os
import io
import csv
import time
import secrets
import logging
import hashlib
import hmac
from datetime import datetime, timezone, timedelta
from aiohttp import web, ClientSession, WSMsgType
import aiosqlite
import bcrypt
import jwt

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger(__name__)

BASE = 'https://mainnet.zklighter.elliot.ai'
BASE_WS = 'wss://mainnet.zklighter.elliot.ai/stream'
GENESIS_MS = 1737072000000
DB_PATH = '/tmp/lighter_tracker.db'
JWT_SECRET = os.environ.get('JWT_SECRET', secrets.token_hex(32))
ADMIN_CODE = os.environ.get('ADMIN_INVITE_CODE', 'LIGHTER2025')

# ── In-memory cache per user ──
# user_cache[user_id] = {trades, funding, positions, market_map, connected, loading, last_update, ws_task}
user_cache = {}

# ── DB ──
async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('''CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            email TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            lighter_token TEXT DEFAULT '',
            created_at INTEGER DEFAULT 0,
            is_admin INTEGER DEFAULT 0
        )''')
        await db.execute('''CREATE TABLE IF NOT EXISTS invite_codes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            code TEXT UNIQUE NOT NULL,
            created_by INTEGER,
            used_by INTEGER DEFAULT NULL,
            created_at INTEGER DEFAULT 0,
            used_at INTEGER DEFAULT 0
        )''')
        await db.commit()
        # Create default invite code
        ts = int(time.time() * 1000)
        await db.execute('INSERT OR IGNORE INTO invite_codes (code, created_by, created_at) VALUES (?, 0, ?)',
                        (ADMIN_CODE, ts))
        await db.commit()
        log.info(f"DB ready. Default invite code: {ADMIN_CODE}")

async def get_user(email=None, user_id=None):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        if email:
            async with db.execute('SELECT * FROM users WHERE email=?', (email,)) as c:
                return await c.fetchone()
        if user_id:
            async with db.execute('SELECT * FROM users WHERE id=?', (user_id,)) as c:
                return await c.fetchone()

async def create_user(email, password, invite_code):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        # Check invite code
        async with db.execute('SELECT * FROM invite_codes WHERE code=? AND used_by IS NULL', (invite_code,)) as c:
            code = await c.fetchone()
        if not code:
            return None, 'Codigo de invitacion invalido o ya usado'
        # Check email not taken
        async with db.execute('SELECT id FROM users WHERE email=?', (email,)) as c:
            if await c.fetchone():
                return None, 'Este email ya esta registrado'
        # Create user
        pw_hash = bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()
        ts = int(time.time() * 1000)
        async with db.execute('INSERT INTO users (email, password_hash, created_at) VALUES (?,?,?)',
                             (email, pw_hash, ts)) as c:
            user_id = c.lastrowid
        # Mark invite as used
        await db.execute('UPDATE invite_codes SET used_by=?, used_at=? WHERE code=?',
                        (user_id, ts, invite_code))
        await db.commit()
        return user_id, None

async def update_token(user_id, token):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('UPDATE users SET lighter_token=? WHERE id=?', (token, user_id))
        await db.commit()

async def create_invite(created_by):
    code = secrets.token_urlsafe(8).upper()
    ts = int(time.time() * 1000)
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('INSERT INTO invite_codes (code, created_by, created_at) VALUES (?,?,?)',
                        (code, created_by, ts))
        await db.commit()
    return code

async def list_invites(created_by):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute('SELECT * FROM invite_codes WHERE created_by=? ORDER BY created_at DESC',
                             (created_by,)) as c:
            return [dict(r) for r in await c.fetchall()]

async def list_users():
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute('SELECT id, email, created_at, lighter_token FROM users ORDER BY created_at') as c:
            return [dict(r) for r in await c.fetchall()]

# ── JWT ──
def make_token(user_id, email):
    return jwt.encode({'user_id': user_id, 'email': email, 'exp': time.time() + 86400 * 30},
                     JWT_SECRET, algorithm='HS256')

def verify_token(token):
    try:
        return jwt.decode(token, JWT_SECRET, algorithms=['HS256'])
    except:
        return None

# ── Auth middleware ──
async def get_current_user(request):
    auth = request.headers.get('Authorization', '')
    if auth.startswith('Bearer '):
        payload = verify_token(auth[7:])
        if payload:
            return payload
    token = request.cookies.get('auth_token')
    if token:
        payload = verify_token(token)
        if payload:
            return payload
    return None

def require_auth(handler):
    async def wrapper(request):
        user = await get_current_user(request)
        if not user:
            return web.json_response({'error': 'No autenticado'}, status=401)
        request['user'] = user
        return await handler(request)
    return wrapper

# ── Lighter data per user ──
def get_cache(user_id):
    if user_id not in user_cache:
        user_cache[user_id] = {
            'trades': {}, 'funding': {}, 'positions': {},
            'market_map': {}, 'connected': False,
            'loading': False, 'initial_load_done': False,
            'last_update': 0, 'last_incremental': 0,
            'ws_task': None, 'session': None
        }
    return user_cache[user_id]

def sym(cache, mid):
    return cache['market_map'].get(str(mid), f'market_{mid}')

def to_ms(dt):
    return int(dt.timestamp() * 1000)

def from_ms(ms):
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc)

def today_start_ms():
    now = datetime.now(timezone.utc)
    return to_ms(now.replace(hour=0, minute=0, second=0, microsecond=0))

def parse_account(token):
    try: return token.split(':')[1]
    except: return None

def parse_trade_csv(text, cache):
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

async def export_call(session, token, account, start_ms, end_ms, etype):
    url = f"{BASE}/api/v1/export?account_index={account}&type={etype}&start_timestamp={start_ms}&end_timestamp={end_ms}"
    try:
        async with session.get(url, headers={'Authorization': token}) as r:
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
        log.debug(f"export_call {etype}: {e}")
        return None

async def load_all_funding(session, token, account, cache, start_ts=None):
    now_ms = int(time.time() * 1000)
    start = start_ts or GENESIS_MS
    cursor = None
    total = 0
    while True:
        url = (f"{BASE}/api/v1/positionFunding"
               f"?account_index={account}&market_id=255&limit=100"
               f"&start_timestamp={start}&end_timestamp={now_ms}")
        if cursor:
            url += f"&cursor={cursor}"
        try:
            async with session.get(url, headers={'Authorization': token}) as r:
                if r.status != 200:
                    break
                data = await r.json()
                items = data.get('position_fundings', [])
                if not items:
                    break
                for item in items:
                    fid = str(item.get('funding_id', ''))
                    mid = str(item.get('market_id', ''))
                    payment = float(item.get('change', 0))
                    ts_val = item.get('timestamp', now_ms)
                    if fid:
                        cache['funding'][fid] = {
                            'id': fid, 'symbol': sym(cache, mid),
                            'side': item.get('position_side', 'long'),
                            'payment': payment, 'rate': item.get('rate', ''),
                            'ts': ts_val
                        }
                        total += 1
                cursor = data.get('next_cursor')
                if not cursor or len(items) < 100:
                    break
                await asyncio.sleep(0.2)
        except Exception as e:
            log.error(f"load_funding: {e}")
            break
    ft = round(sum(f['payment'] for f in cache['funding'].values()), 4)
    log.info(f"[user] Funding: {total} payments, total={ft}")

async def load_positions_for_user(session, token, account, cache):
    try:
        async with session.get(f"{BASE}/api/v1/account?by=index&value={account}",
                              headers={'Authorization': token}) as r:
            if r.status == 200:
                for mid, pos in ((await r.json()).get('positions') or {}).items():
                    cache['positions'][str(mid)] = {
                        'market_id': mid, 'symbol': sym(cache, mid),
                        'side': 'long' if int(pos.get('sign', 1)) > 0 else 'short',
                        'size': float(pos.get('position', 0)),
                        'avg_entry': float(pos.get('avg_entry_price', 0)),
                        'unrealized_pnl': float(pos.get('unrealized_pnl', 0)),
                        'realized_pnl': float(pos.get('realized_pnl', 0)),
                        'liquidation_price': float(pos.get('liquidation_price', 0)),
                    }
    except Exception as e:
        log.error(f"positions: {e}")

async def start_user_sync(user_id, lighter_token):
    cache = get_cache(user_id)
    if cache.get('ws_task') and not cache['ws_task'].done():
        cache['ws_task'].cancel()
    cache['loading'] = True
    cache['initial_load_done'] = False
    cache['trades'] = {}
    cache['funding'] = {}
    cache['positions'] = {}
    task = asyncio.ensure_future(user_sync_loop(user_id, lighter_token))
    cache['ws_task'] = task
    log.info(f"Started sync for user {user_id}")

async def user_sync_loop(user_id, lighter_token):
    cache = get_cache(user_id)
    account = parse_account(lighter_token)
    if not account:
        log.error(f"Invalid token for user {user_id}")
        cache['loading'] = False
        return

    async with ClientSession() as session:
        cache['session'] = session
        # Load market map
        try:
            async with session.get(BASE + '/api/v1/orderBookDetails') as r:
                if r.status == 200:
                    for m in (await r.json()).get('order_book_details', []):
                        cache['market_map'][str(m['market_id'])] = m['symbol']
        except: pass

        await load_positions_for_user(session, lighter_token, account, cache)

        # Historical load — monthly chunks
        log.info(f"[user {user_id}] Historical load starting...")
        now = datetime.now(timezone.utc)
        genesis = from_ms(GENESIS_MS).replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        cur = genesis
        while cur < now:
            nxt = (cur.replace(day=28) + timedelta(days=4)).replace(day=1)
            s, e = to_ms(cur), to_ms(min(nxt, now))
            text = await export_call(session, lighter_token, account, s, e, 'trade')
            if text:
                chunk = parse_trade_csv(text, cache)
                cache['trades'].update(chunk)
            await asyncio.sleep(0.3)
            cur = nxt

        await load_all_funding(session, lighter_token, account, cache)
        await load_positions_for_user(session, lighter_token, account, cache)

        cache['initial_load_done'] = True
        cache['loading'] = False
        wp = sum(1 for t in cache['trades'].values() if t.get('pnl') is not None)
        log.info(f"[user {user_id}] Load done: {len(cache['trades'])} trades ({wp} with PnL), {len(cache['funding'])} funding")

        # Incremental scheduler
        async def scheduler():
            while True:
                await asyncio.sleep(900)
                ts = today_start_ms()
                now_ms = int(time.time() * 1000)
                text = await export_call(session, lighter_token, account, ts, now_ms, 'trade')
                if text:
                    cache['trades'].update(parse_trade_csv(text, cache))
                await asyncio.sleep(0.3)
                await load_all_funding(session, lighter_token, account, cache, start_ts=ts)
                await load_positions_for_user(session, lighter_token, account, cache)
                cache['last_incremental'] = int(time.time() * 1000)

        asyncio.ensure_future(scheduler())

        # WebSocket
        while True:
            try:
                async with session.ws_connect(BASE_WS, heartbeat=60) as ws:
                    cache['connected'] = True
                    await ws.send_json({"type": "subscribe", "channel": f"account_all_trades/{account}", "auth": lighter_token})
                    await ws.send_json({"type": "subscribe", "channel": f"account_all_positions/{account}", "auth": lighter_token})
                    async for msg in ws:
                        if msg.type == WSMsgType.TEXT:
                            try:
                                d = json.loads(msg.data)
                                mt = d.get('type', '')
                                if 'trade' in mt.lower():
                                    td = d.get('trade') or d.get('trades') or d.get('data')
                                    for t in ([td] if isinstance(td, dict) else (td or [])):
                                        tid = str(t.get('trade_id') or t.get('id', ''))
                                        if tid and tid not in cache['trades']:
                                            is_ask = str(t.get('ask_account_id', '')) == str(account)
                                            cache['trades'][tid] = {
                                                'id': tid,
                                                'symbol': sym(cache, str(t.get('market_id', ''))),
                                                'side': 'short' if is_ask else 'long',
                                                'tradeType': 'unknown',
                                                'price': float(t.get('price', 0)),
                                                'size': float(t.get('size', 0)),
                                                'pnl': None,
                                                'fee': float(t.get('taker_fee') or t.get('maker_fee') or 0),
                                                'ts': t.get('timestamp') or int(time.time() * 1000),
                                                'source': 'ws'
                                            }
                                elif 'position' in mt.lower():
                                    pd = d.get('position') or d.get('positions') or d.get('data')
                                    for p in ([pd] if isinstance(pd, dict) else (pd or [])):
                                        mid = str(p.get('market_id', ''))
                                        if mid:
                                            cache['positions'][mid] = {
                                                'market_id': mid, 'symbol': sym(cache, mid),
                                                'side': 'long' if int(p.get('sign', 1)) > 0 else 'short',
                                                'size': float(p.get('position', 0)),
                                                'avg_entry': float(p.get('avg_entry_price', 0)),
                                                'unrealized_pnl': float(p.get('unrealized_pnl', 0)),
                                                'realized_pnl': float(p.get('realized_pnl', 0)),
                                                'liquidation_price': float(p.get('liquidation_price', 0)),
                                            }
                                cache['last_update'] = int(time.time() * 1000)
                            except: pass
                        elif msg.type in (WSMsgType.CLOSED, WSMsgType.ERROR):
                            break
            except Exception as e:
                log.error(f"[user {user_id}] WS: {e}")
            cache['connected'] = False
            await asyncio.sleep(5)

def build_summary(cache):
    trades = cache['trades']
    funding = cache['funding']
    positions = cache['positions']
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
    return {
        'total_pnl': round(tp + ft, 4), 'trade_pnl': tp, 'funding_total': ft,
        'today_pnl': round(today_pnl + today_f, 4),
        'today_trade_pnl': today_pnl, 'today_funding': today_f,
        'total_trades': len(trades), 'closed_trades': len(closes),
        'today_trades': len(today_c), 'wins': wins, 'losses': losses, 'win_rate': wr,
        'by_symbol': list(by_sym.values()),
        'positions': list(positions.values()),
        'connected': cache['connected'],
        'initial_load_done': cache['initial_load_done'],
        'loading': cache['loading'],
        'last_update': cache['last_update']
    }

# ── HTTP handlers ──
def cors(r):
    r.headers['Access-Control-Allow-Origin'] = '*'
    r.headers['Access-Control-Allow-Credentials'] = 'true'
    return r

async def h_options(req):
    return web.Response(headers={
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
        'Access-Control-Allow-Headers': 'Content-Type, Authorization',
        'Access-Control-Allow-Credentials': 'true'
    })

async def h_root(req):
    return cors(web.json_response({'ok': True, 'service': 'Lighter Tracker Multi-User'}))

async def h_register(req):
    try:
        data = await req.json()
        email = (data.get('email') or '').strip().lower()
        password = data.get('password') or ''
        invite = (data.get('invite_code') or '').strip().upper()
        if not email or not password or not invite:
            return cors(web.json_response({'error': 'Todos los campos son obligatorios'}, status=400))
        if len(password) < 6:
            return cors(web.json_response({'error': 'La contrasena debe tener al menos 6 caracteres'}, status=400))
        user_id, error = await create_user(email, password, invite)
        if error:
            return cors(web.json_response({'error': error}, status=400))
        token = make_token(user_id, email)
        return cors(web.json_response({'ok': True, 'token': token, 'user_id': user_id, 'email': email}))
    except Exception as e:
        return cors(web.json_response({'error': str(e)}, status=500))

async def h_login(req):
    try:
        data = await req.json()
        email = (data.get('email') or '').strip().lower()
        password = data.get('password') or ''
        user = await get_user(email=email)
        if not user or not bcrypt.checkpw(password.encode(), user['password_hash'].encode()):
            return cors(web.json_response({'error': 'Email o contrasena incorrectos'}, status=401))
        token = make_token(user['id'], email)
        # Auto-start sync if user has a token
        if user['lighter_token']:
            cache = get_cache(user['id'])
            if not cache['initial_load_done'] and not cache['loading']:
                asyncio.ensure_future(start_user_sync(user['id'], user['lighter_token']))
        return cors(web.json_response({'ok': True, 'token': token, 'user_id': user['id'], 'email': email, 'has_lighter_token': bool(user['lighter_token'])}))
    except Exception as e:
        return cors(web.json_response({'error': str(e)}, status=500))

@require_auth
async def h_set_token(req):
    try:
        data = await req.json()
        lighter_token = (data.get('lighter_token') or '').strip()
        if not lighter_token.startswith('ro:'):
            return cors(web.json_response({'error': 'Token invalido. Debe empezar por ro:'}, status=400))
        user_id = req['user']['user_id']
        await update_token(user_id, lighter_token)
        asyncio.ensure_future(start_user_sync(user_id, lighter_token))
        return cors(web.json_response({'ok': True, 'message': 'Token guardado. Cargando datos...'}))
    except Exception as e:
        return cors(web.json_response({'error': str(e)}, status=500))

@require_auth
async def h_summary(req):
    user_id = req['user']['user_id']
    cache = get_cache(user_id)
    user = await get_user(user_id=user_id)
    if user and user['lighter_token'] and not cache['initial_load_done'] and not cache['loading']:
        asyncio.ensure_future(start_user_sync(user_id, user['lighter_token']))
    return cors(web.json_response(build_summary(cache)))

@require_auth
async def h_trades(req):
    user_id = req['user']['user_id']
    cache = get_cache(user_id)
    limit = int(req.rel_url.query.get('limit', 20000))
    sym_f = req.rel_url.query.get('symbol', '').lower()
    all_t = sorted(cache['trades'].values(), key=lambda t: int(t.get('ts', 0) or 0), reverse=True)
    if sym_f:
        all_t = [t for t in all_t if sym_f in (t.get('symbol') or '').lower()]
    return cors(web.json_response({'trades': all_t[:limit], 'total': len(all_t), 'loading': cache['loading']}))

@require_auth
async def h_funding(req):
    user_id = req['user']['user_id']
    cache = get_cache(user_id)
    all_f = sorted(cache['funding'].values(), key=lambda f: int(f.get('ts', 0) or 0), reverse=True)
    return cors(web.json_response({'funding': all_f, 'total': round(sum(f['payment'] for f in all_f), 4), 'count': len(all_f)}))

@require_auth
async def h_create_invite(req):
    user_id = req['user']['user_id']
    code = await create_invite(user_id)
    return cors(web.json_response({'ok': True, 'code': code}))

@require_auth
async def h_list_invites(req):
    user_id = req['user']['user_id']
    codes = await list_invites(user_id)
    return cors(web.json_response({'invites': codes}))

@require_auth
async def h_me(req):
    user_id = req['user']['user_id']
    cache = get_cache(user_id)
    user = await get_user(user_id=user_id)
    return cors(web.json_response({
        'user_id': user_id,
        'email': req['user']['email'],
        'has_lighter_token': bool(user and user['lighter_token']),
        'initial_load_done': cache['initial_load_done'],
        'loading': cache['loading'],
        'trades_count': len(cache['trades']),
        'connected': cache['connected']
    }))

async def on_start(app):
    await init_db()
    # Resume sync for users who already have tokens
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute('SELECT id, lighter_token FROM users WHERE lighter_token != ""') as c:
            users = await c.fetchall()
    for u in users:
        asyncio.ensure_future(start_user_sync(u['id'], u['lighter_token']))
        log.info(f"Resuming sync for user {u['id']}")

async def on_stop(app):
    for uid, cache in user_cache.items():
        if cache.get('ws_task'):
            cache['ws_task'].cancel()

def create_app():
    app = web.Application()
    app.router.add_get('/', h_root)
    app.router.add_post('/auth/register', h_register)
    app.router.add_post('/auth/login', h_login)
    app.router.add_post('/user/token', h_set_token)
    app.router.add_get('/user/me', h_me)
    app.router.add_get('/user/summary', h_summary)
    app.router.add_get('/user/trades', h_trades)
    app.router.add_get('/user/funding', h_funding)
    app.router.add_post('/user/invite', h_create_invite)
    app.router.add_get('/user/invites', h_list_invites)
    app.router.add_options('/{p:.*}', h_options)
    app.on_startup.append(on_start)
    app.on_cleanup.append(on_stop)
    return app

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 10000))
    log.info(f"Starting on port {port}")
    web.run_app(create_app(), port=port)
