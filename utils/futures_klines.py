import hmac
import time
from hashlib import sha256

from config.api_keys import APIKEY, SECRETKEY
from config.params import KLINE_LIMIT_1M
from utils.density_filter import extremes_from_klines


def _perp_symbol(exchange_upper: str, symbol: str) -> str:
    raw = str(symbol or '')
    if exchange_upper == 'OKX':
        return raw if raw.upper().endswith('-SWAP') else f'{raw}-SWAP'
    if exchange_upper == 'MEXC':
        if '_' in raw:
            return raw
        upper = raw.upper()
        if upper.endswith('USDT') and len(upper) > 4:
            return raw[:-4] + '_USDT'
        return raw
    return raw


def _rows_from_ohlcv(rows) -> list:
    """Приводит свечи к [ts, open, high, low, close, ...]."""
    if not rows:
        return []
    if isinstance(rows, dict):
        times = rows.get('time') or rows.get('t') or []
        opens = rows.get('open') or rows.get('o') or []
        highs = rows.get('high') or rows.get('h') or []
        lows = rows.get('low') or rows.get('l') or []
        closes = rows.get('close') or rows.get('c') or []
        packed = []
        for index, ts in enumerate(times):
            packed.append([
                ts,
                opens[index] if index < len(opens) else 0,
                highs[index] if index < len(highs) else 0,
                lows[index] if index < len(lows) else 0,
                closes[index] if index < len(closes) else 0,
            ])
        return packed
    if isinstance(rows, list) and rows and isinstance(rows[0], dict):
        packed = []
        for row in rows:
            lower = {str(key).lower(): value for key, value in row.items()}
            packed.append([
                lower.get('time') or lower.get('t') or lower.get('opentime') or lower.get('timestamp') or 0,
                lower.get('open') or lower.get('o') or 0,
                lower.get('high') or lower.get('h') or 0,
                lower.get('low') or lower.get('l') or 0,
                lower.get('close') or lower.get('c') or 0,
            ])
        return packed
    return [list(row) for row in rows]


async def fetch_futures_1m_extremes(session, exchange: str, symbol: str) -> tuple:
    """High/Low окна и текущая фьючерсная 1m: high, low, ts, bar_high, bar_low, bar_close."""
    exchange_upper = str(exchange or '').strip().upper()
    try:
        rows, newest_first = await _fetch_futures_1m_rows(session, exchange_upper, symbol)
    except Exception:
        return 0, 0, 0, 0, 0, 0
    klines = _rows_from_ohlcv(rows)
    if not klines:
        return 0, 0, 0, 0, 0, 0
    return extremes_from_klines(klines, newest_first=newest_first)


async def _fetch_futures_1m_rows(session, exchange_upper: str, symbol: str):
    if exchange_upper == 'BYBIT':
        async with session.get(
            'https://api.bybit.com/v5/market/kline',
            params={
                'category': 'linear',
                'symbol': symbol,
                'interval': '1',
                'limit': KLINE_LIMIT_1M,
            },
        ) as response:
            payload = await response.json()
        if payload.get('retCode') != 0:
            return [], True
        return (payload.get('result') or {}).get('list') or [], True

    if exchange_upper == 'OKX':
        async with session.get(
            'https://www.okx.com/api/v5/market/candles',
            params={
                'instId': _perp_symbol('OKX', symbol),
                'bar': '1m',
                'limit': str(KLINE_LIMIT_1M),
            },
        ) as response:
            payload = await response.json()
        if str(payload.get('code')) != '0':
            return [], True
        return payload.get('data') or [], True

    if exchange_upper == 'BITGET':
        async with session.get(
            'https://api.bitget.com/api/v2/mix/market/candles',
            params={
                'symbol': symbol,
                'productType': 'USDT-FUTURES',
                'granularity': '1m',
                'limit': str(KLINE_LIMIT_1M),
            },
        ) as response:
            payload = await response.json()
        if payload.get('code') != '00000':
            return [], False
        return payload.get('data') or [], False

    if exchange_upper == 'BINGX':
        params = {
            'symbol': symbol,
            'interval': '1m',
            'limit': str(KLINE_LIMIT_1M),
            'timestamp': str(int(time.time() * 1000)),
        }
        query = '&'.join(f'{key}={value}' for key, value in params.items())
        signature = hmac.new(
            SECRETKEY.encode('utf-8'),
            query.encode('utf-8'),
            digestmod=sha256,
        ).hexdigest()
        url = f'https://open-api.bingx.com/openApi/swap/v3/quote/klines?{query}&signature={signature}'
        async with session.get(url, headers={'X-BX-APIKEY': APIKEY}) as response:
            payload = await response.json()
        if isinstance(payload, dict) and payload.get('code') not in (0, '0', None):
            return [], True
        data = payload.get('data') if isinstance(payload, dict) else payload
        return data or [], True

    if exchange_upper == 'MEXC':
        inst = _perp_symbol('MEXC', symbol)
        url = f'https://contract.mexc.com/api/v1/contract/kline/{inst}'
        async with session.get(url, params={'interval': 'Min1'}) as response:
            payload = await response.json()
        data = payload.get('data') if isinstance(payload, dict) else payload
        return data or {}, False

    if exchange_upper == 'BINANCE':
        async with session.get(
            'https://fapi.binance.com/fapi/v1/klines',
            params={'symbol': symbol, 'interval': '1m', 'limit': KLINE_LIMIT_1M},
        ) as response:
            data = await response.json()
        if isinstance(data, dict):
            return [], False
        return data or [], False

    return [], False
