"""Проторгованный объём по конкретной цене стены."""
import asyncio
import hmac
import math
import time
from hashlib import sha256

import aiohttp

from config.params import TRADE_LOOKBACK_SEC
from utils.density_filter import _density_float

try:
    from config.api_keys import APIKEY, SECRETKEY
except ImportError:
    APIKEY = ''
    SECRETKEY = ''

_cache = {}
_cache_lock = asyncio.Lock()
_CACHE_TTL = 3.0
_MAX_PAGES = 15


def prices_match(trade_price: float, wall_price: float) -> bool:
    if trade_price <= 0 or wall_price <= 0:
        return False
    return math.isclose(trade_price, wall_price, rel_tol=1e-8, abs_tol=0.0)


def lookback_start_ts(density: dict, now: float | None = None) -> float:
    now = time.time() if now is None else now
    start = now - TRADE_LOOKBACK_SEC
    last_checked = density.get('last_checked_ts')
    if last_checked is None:
        return start
    try:
        return max(start, float(last_checked) - 2.0)
    except (TypeError, ValueError):
        return start


def _quote(price: float, qty: float) -> float:
    return price * qty if price > 0 and qty > 0 else 0.0


async def quote_volume_at_price(
    exchange: str,
    symbol: str,
    wall_price: float,
    start_ts: float,
    end_ts: float | None = None,
) -> float | None:
    """USDT-объём сделок по цене стены за окно [start_ts, end_ts]. None если ленту не получили."""
    end_ts = time.time() if end_ts is None else end_ts
    trades = await _recent_trades(exchange, symbol, start_ts, end_ts)
    if trades is None:
        return None
    total = 0.0
    for price, qty, ts in trades:
        if ts < start_ts or ts > end_ts:
            continue
        if prices_match(price, wall_price):
            total += _quote(price, qty)
    return total


async def eaten_ratio_for_wall(exchange: str, density: dict) -> float | None:
    wall = _density_float(density, 'price', 0)
    size = _density_float(density, 'order_size', 0)
    symbol = density.get('symbol')
    if wall <= 0 or size <= 0 or not symbol:
        return None
    now = time.time()
    traded = await quote_volume_at_price(
        exchange, symbol, wall, lookback_start_ts(density, now), now,
    )
    if traded is None:
        return None
    return traded / size


async def _recent_trades(exchange: str, symbol: str, start_ts: float, end_ts: float):
    key = (str(exchange).lower(), str(symbol), round(start_ts, 0))
    async with _cache_lock:
        hit = _cache.get(key)
        if hit and hit['expires'] > time.time():
            return hit['trades']

    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=12)) as session:
            name = str(exchange).strip().lower()
            if name == 'binance':
                trades = await _binance_trades(session, symbol, start_ts, end_ts)
            elif name == 'mexc':
                trades = await _mexc_trades(session, symbol, start_ts, end_ts)
            elif name == 'bybit':
                trades = await _bybit_trades(session, symbol)
            elif name == 'okx':
                trades = await _okx_trades(session, symbol)
            elif name == 'bitget':
                trades = await _bitget_trades(session, symbol)
            elif name == 'bingx':
                trades = await _bingx_trades(session, symbol)
            else:
                trades = None
    except Exception as e:
        print(f"⚠️ {exchange}: ошибка ленты сделок {symbol}: {e}")
        trades = None

    async with _cache_lock:
        _cache[key] = {'trades': trades, 'expires': time.time() + _CACHE_TTL}
        if len(_cache) > 80:
            expired = [k for k, v in _cache.items() if v['expires'] <= time.time()]
            for old in expired:
                _cache.pop(old, None)
    return trades


def _ms(ts: float) -> int:
    return int(ts * 1000)


async def _get_json(session, url, params=None, headers=None):
    async with session.get(url, params=params, headers=headers) as response:
        if response.status != 200:
            return None
        return await response.json(content_type=None)


def _parse_trade(price, qty, ts) -> tuple | None:
    try:
        price = float(price)
        qty = float(qty)
        ts = float(ts)
        if ts > 1e12:
            ts /= 1000.0
        if price <= 0 or qty <= 0 or ts <= 0:
            return None
        return price, qty, ts
    except (TypeError, ValueError):
        return None


async def _binance_trades(session, symbol, start_ts, end_ts):
    url = 'https://api.binance.com/api/v3/aggTrades'
    trades = []
    from_id = None
    start_ms, end_ms = _ms(start_ts), _ms(end_ts)
    for _ in range(_MAX_PAGES):
        params = {'symbol': symbol, 'limit': 1000, 'endTime': end_ms}
        if from_id is None:
            params['startTime'] = start_ms
        else:
            params['fromId'] = from_id
        data = await _get_json(session, url, params)
        if not isinstance(data, list) or not data:
            break
        last_id = None
        for row in data:
            last_id = row.get('a', last_id)
            parsed = _parse_trade(row.get('p'), row.get('q'), row.get('T'))
            if parsed:
                trades.append(parsed)
        if len(data) < 1000:
            break
        if last_id is None:
            break
        from_id = int(last_id) + 1
        if _parse_trade(data[-1].get('p'), data[-1].get('q'), data[-1].get('T')):
            if data[-1].get('T', 0) >= end_ms:
                break
    return trades


async def _mexc_trades(session, symbol, start_ts, end_ts):
    url = 'https://api.mexc.com/api/v3/aggTrades'
    trades = []
    from_id = None
    start_ms, end_ms = _ms(start_ts), _ms(end_ts)
    for _ in range(_MAX_PAGES):
        params = {'symbol': symbol, 'limit': 1000}
        if from_id is None:
            params['startTime'] = start_ms
            params['endTime'] = end_ms
        else:
            params['fromId'] = from_id
        data = await _get_json(session, url, params)
        if not isinstance(data, list) or not data:
            # MEXC иногда не принимает окно времени — берём хвост ленты
            if from_id is None and not trades:
                data = await _get_json(session, url, {'symbol': symbol, 'limit': 1000})
                if not isinstance(data, list) or not data:
                    break
            else:
                break
        last_id = None
        for row in data:
            last_id = row.get('a', last_id)
            parsed = _parse_trade(row.get('p'), row.get('q'), row.get('T'))
            if parsed:
                trades.append(parsed)
        if len(data) < 1000:
            break
        if last_id is None:
            break
        from_id = int(last_id) + 1
    return trades


async def _bybit_trades(session, symbol):
    url = 'https://api.bybit.com/v5/market/recent-trade'
    data = await _get_json(session, url, {'category': 'spot', 'symbol': symbol, 'limit': 1000})
    rows = ((data or {}).get('result') or {}).get('list') or []
    trades = []
    for row in rows:
        parsed = _parse_trade(row.get('price'), row.get('size'), row.get('time'))
        if parsed:
            trades.append(parsed)
    return trades


async def _okx_trades(session, symbol):
    url = 'https://www.okx.com/api/v5/market/history-trades'
    trades = []
    after = None
    for _ in range(_MAX_PAGES):
        params = {'instId': symbol, 'type': '1', 'limit': '100'}
        if after:
            params['after'] = after
        data = await _get_json(session, url, params)
        rows = (data or {}).get('data') or []
        if not rows:
            break
        for row in rows:
            parsed = _parse_trade(row.get('px'), row.get('sz'), row.get('ts'))
            if parsed:
                trades.append(parsed)
        after = rows[-1].get('tradeId')
        if not after or len(rows) < 100:
            break
    return trades


async def _bitget_trades(session, symbol):
    url = 'https://api.bitget.com/api/v2/spot/market/fills'
    data = await _get_json(session, url, {'symbol': symbol, 'limit': '500'})
    rows = (data or {}).get('data') or []
    trades = []
    for row in rows:
        parsed = _parse_trade(row.get('price'), row.get('size'), row.get('ts'))
        if parsed:
            trades.append(parsed)
    return trades


async def _bingx_trades(session, symbol):
    params = {
        'symbol': symbol,
        'limit': '200',
        'timestamp': str(int(time.time() * 1000)),
    }
    query = '&'.join(f'{key}={value}' for key, value in params.items())
    headers = {}
    if SECRETKEY:
        signature = hmac.new(SECRETKEY.encode('utf-8'), query.encode('utf-8'), sha256).hexdigest()
        query = f'{query}&signature={signature}'
        headers['X-BX-APIKEY'] = APIKEY
    url = f'https://open-api.bingx.com/openApi/spot/v1/market/trades?{query}'
    data = await _get_json(session, url, headers=headers)
    rows = (data or {}).get('data') or []
    if isinstance(rows, dict):
        rows = rows.get('trades') or rows.get('list') or []
    trades = []
    for row in rows:
        parsed = _parse_trade(
            row.get('price') or row.get('p'),
            row.get('qty') or row.get('q') or row.get('volume'),
            row.get('time') or row.get('T') or row.get('ts'),
        )
        if parsed:
            trades.append(parsed)
    return trades
