"""Шаг цены спота и фьючерса в % от цены стены: (tick / price) × 100."""

import asyncio
import hmac
import time
from hashlib import sha256
from threading import Lock

import aiohttp

_lock = Lock()
_cache: dict[str, dict[str, dict]] = {}


def _compact(symbol) -> str:
    return str(symbol or '').replace('-', '').replace('_', '').upper()


def _ex_key(exchange) -> str:
    return str(exchange or '').strip().lower()


def _as_tick(raw) -> float | None:
    try:
        value = float(raw)
    except (TypeError, ValueError):
        return None
    return value if value > 0 else None


def _tick_from_precision(raw) -> float | None:
    try:
        places = int(float(raw))
    except (TypeError, ValueError):
        return None
    if places < 0 or places > 16:
        return None
    return 10.0 ** (-places)


def _first_tick(*values) -> float | None:
    for value in values:
        tick = _as_tick(value)
        if tick is not None:
            return tick
    return None


def tick_to_pct(tick, price) -> float | None:
    tick = _as_tick(tick)
    try:
        price = float(price)
    except (TypeError, ValueError):
        price = 0.0
    if tick is None or price <= 0:
        return None
    return round((tick / price) * 100.0, 6)


def _binance_filter_tick(info) -> float | None:
    for item in info.get('filters') or []:
        if str(item.get('filterType') or '') == 'PRICE_FILTER':
            return _as_tick(item.get('tickSize'))
    return _as_tick(info.get('tickSize'))


def _binance_like_tick(info) -> float | None:
    tick = _binance_filter_tick(info)
    if tick is not None:
        return tick
    return _tick_from_precision(
        info.get('quoteAssetPrecision')
        or info.get('quotePrecision')
        or info.get('pricePrecision')
    )


def _put(bucket: dict, symbol, market: str, tick: float | None) -> None:
    key = _compact(symbol)
    if not key or tick is None:
        return
    row = bucket.setdefault(key, {})
    row[market] = tick


def get_ticks(exchange, symbol) -> dict:
    with _lock:
        return dict((_cache.get(_ex_key(exchange)) or {}).get(_compact(symbol)) or {})


def tick_pcts_for(exchange, symbol, price) -> tuple:
    ticks = get_ticks(exchange, symbol)
    return tick_to_pct(ticks.get('spot'), price), tick_to_pct(ticks.get('fut'), price)


def attach_price_steps(density, exchange=None) -> None:
    if not density:
        return
    exch = exchange or density.get('exchange')
    symbol = density.get('symbol')
    try:
        price = float(density.get('price') or density.get('current_price') or 0)
    except (TypeError, ValueError):
        price = 0.0
    spot_pct, fut_pct = tick_pcts_for(exch, symbol, price)
    if spot_pct is not None:
        density['spot_tick_pct'] = spot_pct
    if fut_pct is not None:
        density['fut_tick_pct'] = fut_pct


def fill_row_price_steps(row) -> dict:
    """Подставить шаг цены в строку журнала, если при записи кэш ещё был пуст."""
    if not row:
        return row
    spot = row.get('spot_tick_pct')
    fut = row.get('fut_tick_pct')
    if spot not in (None, '') and fut not in (None, ''):
        return row
    price = row.get('wall_price') or row.get('last_price')
    spot_pct, fut_pct = tick_pcts_for(row.get('exchange'), row.get('symbol'), price)
    if spot in (None, '') and spot_pct is not None:
        row['spot_tick_pct'] = spot_pct
    if fut in (None, '') and fut_pct is not None:
        row['fut_tick_pct'] = fut_pct
    return row


async def ensure_price_steps(exchange, symbol=None) -> None:
    if symbol and get_ticks(exchange, symbol):
        return
    with _lock:
        loaded = bool(_cache.get(_ex_key(exchange)))
    if loaded:
        return
    await refresh_price_steps(exchange)


def _store(exchange: str, bucket: dict) -> None:
    with _lock:
        current = _cache.setdefault(_ex_key(exchange), {})
        current.update(bucket)


async def _json(session, url, params=None, headers=None):
    timeout = aiohttp.ClientTimeout(total=25)
    async with session.get(url, params=params, headers=headers, timeout=timeout) as response:
        if response.status != 200:
            return None
        return await response.json(content_type=None)


def _parse_binance(spot_data, fut_data) -> dict:
    bucket = {}
    for info in (spot_data or {}).get('symbols') or []:
        _put(bucket, info.get('symbol'), 'spot', _binance_like_tick(info))
    for info in (fut_data or {}).get('symbols') or []:
        _put(bucket, info.get('symbol'), 'fut', _binance_like_tick(info))
    return bucket


def _parse_bybit(spot_data, fut_data) -> dict:
    bucket = {}
    for info in ((spot_data or {}).get('result') or {}).get('list') or []:
        filt = info.get('priceFilter') or {}
        _put(bucket, info.get('symbol'), 'spot', _first_tick(filt.get('tickSize'), info.get('tickSize')))
    for info in ((fut_data or {}).get('result') or {}).get('list') or []:
        filt = info.get('priceFilter') or {}
        _put(bucket, info.get('symbol'), 'fut', _first_tick(filt.get('tickSize'), info.get('tickSize')))
    return bucket


def _parse_okx(spot_data, fut_data) -> dict:
    bucket = {}
    for info in (spot_data or {}).get('data') or []:
        _put(bucket, info.get('instId'), 'spot', _as_tick(info.get('tickSz')))
    for info in (fut_data or {}).get('data') or []:
        inst = str(info.get('instId') or '').replace('-SWAP', '')
        _put(bucket, inst, 'fut', _as_tick(info.get('tickSz')))
    return bucket


def _parse_bitget(spot_data, fut_data) -> dict:
    bucket = {}
    for info in (spot_data or {}).get('data') or []:
        tick = _first_tick(info.get('tickSz'), info.get('tickSize'))
        if tick is None:
            tick = _tick_from_precision(info.get('pricePrecision'))
        _put(bucket, info.get('symbol'), 'spot', tick)
    for info in (fut_data or {}).get('data') or []:
        tick = _first_tick(info.get('tickSz'), info.get('tickSize'))
        if tick is None:
            place = info.get('pricePlace')
            step = _as_tick(info.get('priceEndStep')) or 1.0
            base = _tick_from_precision(place)
            if base is not None:
                tick = step * base
        _put(bucket, info.get('symbol'), 'fut', tick)
    return bucket


def _parse_mexc(spot_data, fut_data) -> dict:
    bucket = {}
    for info in (spot_data or {}).get('symbols') or []:
        _put(bucket, info.get('symbol'), 'spot', _binance_like_tick(info))
    for info in (fut_data or {}).get('data') or []:
        tick = _first_tick(info.get('priceUnit'), info.get('tickSize'))
        if tick is None:
            tick = _tick_from_precision(info.get('priceScale'))
        _put(bucket, str(info.get('symbol') or '').replace('_', ''), 'fut', tick)
    return bucket


def _parse_bingx(spot_data, fut_data) -> dict:
    bucket = {}
    symbols = ((spot_data or {}).get('data') or {})
    if isinstance(symbols, dict):
        symbols = symbols.get('symbols') or []
    for info in symbols or []:
        _put(bucket, info.get('symbol'), 'spot', _first_tick(info.get('tickSize'), info.get('minTickSize')))
    fut_list = (fut_data or {}).get('data') or []
    if isinstance(fut_list, dict):
        fut_list = fut_list.get('contracts') or fut_list.get('list') or []
    for info in fut_list or []:
        tick = _first_tick(info.get('tickSize'), info.get('minTickSize'))
        if tick is None:
            tick = _tick_from_precision(info.get('pricePrecision'))
        _put(bucket, info.get('symbol'), 'fut', tick)
    return bucket


def _bingx_headers_and_query():
    try:
        from config.api_keys import APIKEY, SECRETKEY
    except Exception:
        return {}, ''
    timestamp = str(int(time.time() * 1000))
    query = f'timestamp={timestamp}'
    signature = hmac.new(
        str(SECRETKEY).encode('utf-8'),
        query.encode('utf-8'),
        digestmod=sha256,
    ).hexdigest()
    headers = {'X-BX-APIKEY': APIKEY} if APIKEY else {}
    return headers, f'{query}&signature={signature}'


async def _fetch_binance(session) -> dict:
    spot, fut = await asyncio.gather(
        _json(session, 'https://api.binance.com/api/v3/exchangeInfo'),
        _json(session, 'https://fapi.binance.com/fapi/v1/exchangeInfo'),
    )
    return _parse_binance(spot, fut)


async def _fetch_bybit(session) -> dict:
    params = {'limit': '1000'}
    spot, fut = await asyncio.gather(
        _json(session, 'https://api.bybit.com/v5/market/instruments-info', {'category': 'spot', **params}),
        _json(session, 'https://api.bybit.com/v5/market/instruments-info', {'category': 'linear', **params}),
    )
    return _parse_bybit(spot, fut)


async def _fetch_okx(session) -> dict:
    spot, fut = await asyncio.gather(
        _json(session, 'https://www.okx.com/api/v5/public/instruments', {'instType': 'SPOT'}),
        _json(session, 'https://www.okx.com/api/v5/public/instruments', {'instType': 'SWAP'}),
    )
    return _parse_okx(spot, fut)


async def _fetch_bitget(session) -> dict:
    spot, fut = await asyncio.gather(
        _json(session, 'https://api.bitget.com/api/v2/spot/public/symbols'),
        _json(session, 'https://api.bitget.com/api/v2/mix/market/contracts', {'productType': 'USDT-FUTURES'}),
    )
    return _parse_bitget(spot, fut)


async def _fetch_mexc(session) -> dict:
    spot, fut = await asyncio.gather(
        _json(session, 'https://api.mexc.com/api/v3/exchangeInfo'),
        _json(session, 'https://contract.mexc.com/api/v1/contract/detail'),
    )
    return _parse_mexc(spot, fut)


async def _fetch_bingx(session) -> dict:
    headers, query = _bingx_headers_and_query()
    spot_url = f'https://open-api.bingx.com/openApi/spot/v1/common/symbols?{query}' if query else (
        'https://open-api.bingx.com/openApi/spot/v1/common/symbols'
    )
    fut_url = f'https://open-api.bingx.com/openApi/swap/v2/quote/contracts?{query}' if query else (
        'https://open-api.bingx.com/openApi/swap/v2/quote/contracts'
    )
    spot, fut = await asyncio.gather(
        _json(session, spot_url, headers=headers or None),
        _json(session, fut_url, headers=headers or None),
    )
    return _parse_bingx(spot, fut)


_FETCHERS = {
    'binance': _fetch_binance,
    'bybit': _fetch_bybit,
    'okx': _fetch_okx,
    'bitget': _fetch_bitget,
    'mexc': _fetch_mexc,
    'bingx': _fetch_bingx,
}


async def refresh_price_steps(exchange: str, symbols=None) -> int:
    fetcher = _FETCHERS.get(_ex_key(exchange))
    if fetcher is None:
        return 0
    try:
        timeout = aiohttp.ClientTimeout(total=30)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            bucket = await fetcher(session)
    except Exception as error:
        print(f'⚠️ {exchange}: шаг цены не загружен — {error}')
        return 0
    if not bucket:
        print(f'⚠️ {exchange}: шаг цены пустой')
        return 0
    _store(exchange, bucket)
    print(f'📐 {exchange}: шаг цены для {len(bucket)} символов')
    return len(bucket)
