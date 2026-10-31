import asyncio
import re
import time
from datetime import datetime
from pathlib import Path

from utils.btc_context import BTC_EXCHANGE, BTC_MARKET, BTC_SYMBOL, btc_journal_moves
from utils.density_filter import _density_float, is_buy_wall

CHART_DIR = Path('data') / 'charts'
CHART_ID_RE = re.compile(r'^[A-Za-z0-9._-]+$')


def _safe_token(value) -> str:
    text = re.sub(r'[^A-Za-z0-9_-]+', '_', str(value or ''))
    return text.strip('_') or 'x'


def make_chart_id(exchange, symbol, side, wall_price) -> str:
    ts = datetime.now().strftime('%Y%m%d_%H%M%S_%f')
    side_tag = 'bid' if str(side) in ('ПОКУПКА', 'bid', 'BUY') else 'ask'
    price = str(wall_price).replace('.', 'p').replace('-', 'm')
    return f"{ts}_{_safe_token(exchange)}_{_safe_token(symbol)}_{side_tag}_{_safe_token(price)}"


def chart_path(chart_id: str) -> Path | None:
    if not chart_id or not CHART_ID_RE.fullmatch(chart_id):
        return None
    path = (CHART_DIR / f'{chart_id}.png').resolve()
    try:
        path.relative_to(CHART_DIR.resolve())
    except ValueError:
        return None
    return path


def bounce_level_price(density, outcome=None) -> float:
    """Цена жёлтой линии: уровень этого отскока (отск.%). Без отскока — 0."""
    outcome_text = '' if outcome is None else str(outcome)
    if outcome_text == 'отскок фьюч':
        stored = _density_float(density, 'last_fut_bounce_price', 0)
        if stored > 0:
            return stored
        touch = _density_float(density, 'fut_touch_price', 0)
        move = _density_float(density, 'last_fut_bounce_move', 0)
        if touch <= 0 or move <= 0:
            return 0.0
        if is_buy_wall(density):
            return touch * (1 + move / 100.0)
        denom = 1 + move / 100.0
        return touch / denom if denom > 0 else 0.0
    if outcome is not None and outcome_text != 'отскок':
        return 0.0
    stored = _density_float(density, 'last_bounce_price', 0)
    if stored > 0:
        return stored
    wall = _density_float(density, 'price', 0)
    move = _density_float(density, 'last_bounce_move', 0)
    min_pct = _density_float(density, 'min_percent', 0)
    if wall <= 0 or move <= 0:
        return 0.0
    if is_buy_wall(density):
        touch = wall * (1 + min_pct / 100.0)
        return touch * (1 + move / 100.0) if touch > 0 else 0.0
    touch = wall * (1 - min_pct / 100.0)
    denom = 1 + move / 100.0
    return touch / denom if touch > 0 and denom > 0 else 0.0


def density_found_at(density) -> float:
    """Unix-время первого появления стены."""
    for key in ('first_seen_ts', 'bid_first_seen', 'ask_first_seen'):
        raw = (density or {}).get(key)
        try:
            ts = float(raw)
        except (TypeError, ValueError):
            continue
        if ts > 1e12:
            ts /= 1000.0
        if ts > 1e9:
            return ts
    life = _density_float(density, 'life_time', 0)
    if life > 1e9:
        return life
    if life > 0:
        return time.time() - life
    return 0.0


async def _candles_or_none(exchange: str, symbol: str, interval: str, get_candles, market: str = 'spot'):
    try:
        df = await get_candles(exchange, symbol, interval, 100, market)
    except Exception as error:
        print(f'⚠️ Свечи {market} {interval} для графика {exchange} {symbol}: {error}')
        return None
    if df is None or getattr(df, 'empty', True):
        return None
    return df


async def _write_chart_png(
    chart_id: str, df, title: str, wall, bounce, move_pct, found_at, generate_bytes,
    entry=0,
) -> bool:
    path = chart_path(chart_id)
    if path is None or df is None:
        return False
    try:
        png = await asyncio.to_thread(
            generate_bytes,
            df,
            title,
            wall if wall > 0 else None,
            bounce_line=bounce if bounce > 0 else None,
            bounce_pct=move_pct if move_pct > 0 else None,
            found_at=found_at or None,
            entry_line=entry if entry > 0 else None,
        )
    except Exception as error:
        print(f'⚠️ Ошибка генерации графика {chart_id}: {error}')
        return False
    if not png:
        return False
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(png)
    print(f'🖼 график сохранён {path.name}')
    return True


async def save_outcome_chart(exchange: str, density: dict, chart_id: str, outcome=None) -> dict:
    empty = {'ok': False, 'btc_move': '', 'btc_touch_12x5m': '', 'btc_bounce_nx5m': ''}
    if chart_path(chart_id) is None:
        return empty
    try:
        from app import ChartGenerator, UniversalChartGenerator
    except Exception as error:
        print(f'⚠️ Не удалось импортировать генератор графика: {error}')
        return empty

    symbol = density.get('symbol')
    if not symbol:
        return empty

    get_candles = UniversalChartGenerator.get_candles_safe
    df_s1, df_s5, df_f1, df_f5, df_b1, df_b5 = await asyncio.gather(
        _candles_or_none(exchange, symbol, '1m', get_candles, 'spot'),
        _candles_or_none(exchange, symbol, '5m', get_candles, 'spot'),
        _candles_or_none(exchange, symbol, '1m', get_candles, 'futures'),
        _candles_or_none(exchange, symbol, '5m', get_candles, 'futures'),
        _candles_or_none(BTC_EXCHANGE, BTC_SYMBOL, '1m', get_candles, BTC_MARKET),
        _candles_or_none(BTC_EXCHANGE, BTC_SYMBOL, '5m', get_candles, BTC_MARKET),
    )
    moves = btc_journal_moves(df_b5, density, outcome)
    btc_move_value = '' if moves.get('btc_move') is None else moves['btc_move']
    btc_touch_value = '' if moves.get('btc_touch_12x5m') is None else moves['btc_touch_12x5m']
    btc_bounce_value = '' if moves.get('btc_bounce_nx5m') is None else moves['btc_bounce_nx5m']

    wall = _density_float(density, 'price', 0)
    bounce = bounce_level_price(density, outcome=outcome)
    is_fut = str(outcome) == 'отскок фьюч'
    if is_fut:
        spot_bounce = 0.0
        spot_move = 0.0
        fut_bounce = bounce
        fut_move = _density_float(density, 'last_fut_bounce_move', 0) if bounce > 0 else 0.0
    else:
        spot_bounce = bounce
        spot_move = _density_float(density, 'last_bounce_move', 0) if bounce > 0 else 0.0
        fut_bounce = bounce
        fut_move = spot_move
    found_at = density_found_at(density)
    fut_entry = _density_float(density, 'fut_touch_price', 0)
    generate_bytes = ChartGenerator.generate_full_chart_bytes
    ok_s1, ok_s5, ok_f1, ok_f5, ok_b1, ok_b5 = await asyncio.gather(
        _write_chart_png(
            f'{chart_id}_1m', df_s1, f'{exchange} {symbol} спот 1m',
            wall, spot_bounce, spot_move, found_at, generate_bytes,
        ),
        _write_chart_png(
            chart_id, df_s5, f'{exchange} {symbol} спот 5m',
            wall, spot_bounce, spot_move, found_at, generate_bytes,
        ),
        _write_chart_png(
            f'{chart_id}_fut_1m', df_f1, f'{exchange} {symbol} фьючерс 1m',
            wall, fut_bounce, fut_move, found_at, generate_bytes,
            entry=fut_entry,
        ),
        _write_chart_png(
            f'{chart_id}_fut', df_f5, f'{exchange} {symbol} фьючерс 5m',
            wall, fut_bounce, fut_move, found_at, generate_bytes,
            entry=fut_entry,
        ),
        _write_chart_png(
            f'{chart_id}_btc_1m', df_b1, '',
            0, 0, 0, found_at, generate_bytes,
        ),
        _write_chart_png(
            f'{chart_id}_btc_5m', df_b5, '',
            0, 0, 0, found_at, generate_bytes,
        ),
    )
    return {
        'ok': bool(ok_s1 or ok_s5 or ok_f1 or ok_f5 or ok_b1 or ok_b5),
        'btc_move': btc_move_value,
        'btc_touch_12x5m': btc_touch_value,
        'btc_bounce_nx5m': btc_bounce_value,
    }
