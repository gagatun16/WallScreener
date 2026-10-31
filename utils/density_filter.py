from statistics import median
import time

from config.params import (
    MIN_ORDER_SIZE,
    COEFFICIENT_SIZE_OF_PLOTNOST,
    MIN_MEDIAN_VOLUME_CANDLES,
    BOUNCE_MIN_PERCENT,
    KLINE_LIMIT_1M,
)


def median_usdt_volume(volumes) -> float:
    """Медиана объёма в USDT по закрытым 5m-свечам (минимум 12)."""
    values = []
    for volume in volumes:
        try:
            amount = float(volume)
        except (TypeError, ValueError):
            continue
        if amount > 0:
            values.append(amount)
    if len(values) < MIN_MEDIAN_VOLUME_CANDLES:
        return 0.0
    return float(median(values))


def true_range_pct(high: float, low: float, prev_close: float) -> float:
    """True Range в процентах от предыдущего закрытия."""
    if prev_close is None or prev_close <= 0:
        return 0.0
    true_range = max(high - low, abs(high - prev_close), abs(low - prev_close))
    return true_range / prev_close * 100


def median_tr_pct(candles_oldest_first, bar_count: int, min_bars: int) -> float:
    """Медиана TR% по закрытым свечам. Первая свеча — только предыдущее закрытие."""
    tr_values = []
    for i in range(1, len(candles_oldest_first)):
        try:
            high, low, _close = candles_oldest_first[i]
            prev_close = candles_oldest_first[i - 1][2]
            tr_pct = true_range_pct(float(high), float(low), float(prev_close))
        except (TypeError, ValueError, IndexError):
            continue
        if tr_pct > 0:
            tr_values.append(tr_pct)
    if bar_count:
        tr_values = tr_values[-bar_count:]
    if len(tr_values) < min_bars:
        return 0.0
    return float(median(tr_values))


def median_tr_pct_from_klines(klines, newest_first: bool, bar_count: int, min_bars: int) -> float:
    """TR% из сырых свечей биржи: high=2, low=3, close=4. Текущая свеча отбрасывается."""
    if not klines or len(klines) < min_bars + 2:
        return 0.0
    history = list(reversed(klines[1:])) if newest_first else list(klines[:-1])
    ohlc = []
    for candle in history:
        try:
            ohlc.append((float(candle[2]), float(candle[3]), float(candle[4])))
        except (TypeError, ValueError, IndexError):
            continue
    return median_tr_pct(ohlc, bar_count, min_bars)


def is_density_wall(order_price: float, order_qty: float, avg_volume_usdt: float) -> bool:
    """Стена: размер в USDT >= 3× медианы объёма 5m в USDT и больше $50_000."""
    if avg_volume_usdt is None or avg_volume_usdt <= 0:
        return False
    order_value = order_price * order_qty
    return (
        order_value > MIN_ORDER_SIZE
        and order_value >= COEFFICIENT_SIZE_OF_PLOTNOST * avg_volume_usdt
    )


def collect_density_walls(order_side, side: str, symbol: str, avg_volume_usdt: float) -> list:
    """Все заявки в стакане, которые проходят правило стены."""
    found = []
    seen_prices = set()
    for order in order_side or []:
        try:
            order_price = float(order[0])
            order_size = float(order[1])
        except (TypeError, ValueError, IndexError):
            continue
        if order_price in seen_prices:
            continue
        if not is_density_wall(order_price, order_size, avg_volume_usdt):
            continue
        seen_prices.add(order_price)
        found.append({
            'side': side,
            'symbol': symbol,
            'price': order_price,
            'life_time': time.time(),
        })
    return found


def iter_density_hits(hit):
    """Разворачивает одну стену или список стен из анализа стакана."""
    if not hit:
        return
    if isinstance(hit, list):
        for item in hit:
            if item:
                yield item
        return
    yield hit


def _density_float(density, key, default=0.0) -> float:
    value = density.get(key, default)
    try:
        if isinstance(value, str):
            value = value.replace(',', '.').replace(' ', '').replace('%', '')
        return float(value)
    except (TypeError, ValueError):
        return default


def current_candle_extra(candle):
    """open_ts (сек), high, low из строки OHLCV: 0=openTime, 2=high, 3=low."""
    try:
        open_ts = float(candle[0])
        if open_ts > 1e12:
            open_ts /= 1000.0
        return open_ts, float(candle[2]), float(candle[3])
    except (TypeError, ValueError, IndexError):
        return 0.0, 0.0, 0.0


def is_buy_wall(density) -> bool:
    side = str((density or {}).get('side', ''))
    return side in ('ПОКУПКА', 'bid', 'BUY')


def wall_wick_percent(density) -> float:
    """Расстояние до стены по фитилю 5m: BID → Low, ASK → High."""
    wall = _density_float(density, 'price', 0)
    close_pct = _density_float(density, 'percent', 100)
    high = _density_float(density, 'candle_high', 0)
    low = _density_float(density, 'candle_low', 0)
    if wall <= 0 or high <= 0 or low <= 0:
        return close_pct
    extreme = low if is_buy_wall(density) else high
    if extreme <= 0:
        return close_pct
    return abs((wall * 100) / extreme - 100)


def wall_wick_1m_percent(density) -> float | None:
    """Расстояние до стены по фитилю текущей 1m: BID → Low, ASK → High."""
    wall = _density_float(density, 'price', 0)
    high = _density_float(density, 'candle_1m_bar_high', 0)
    low = _density_float(density, 'candle_1m_bar_low', 0)
    if wall <= 0 or high <= 0 or low <= 0:
        return None
    extreme = low if is_buy_wall(density) else high
    if extreme <= 0:
        return None
    return abs((wall * 100) / extreme - 100)


def wall_close_1m_percent(density) -> float | None:
    """Расстояние до стены по close текущей 1m."""
    wall = _density_float(density, 'price', 0)
    close = _density_float(density, 'candle_1m_close', 0)
    if wall <= 0 or close <= 0:
        return None
    return abs((wall * 100) / close - 100)


def wall_fut_close_1m_percent(density) -> float | None:
    """Расстояние до стены по close текущей фьючерсной 1m."""
    wall = _density_float(density, 'price', 0)
    close = _density_float(density, 'candle_fut_1m_close', 0)
    if wall <= 0 or close <= 0:
        return None
    return abs((wall * 100) / close - 100)


def should_watch_1m(density, wick=None) -> bool:
    """1m нужны с первой минуты: подходы считаются по минутной свече."""
    return bool(density)


def extremes_from_klines(klines, newest_first: bool, count: int | None = None) -> tuple:
    """max High, min Low окна; open_ts, High, Low, Close самой новой 1m."""
    if not klines:
        return 0.0, 0.0, 0.0, 0.0, 0.0, 0.0
    window_size = count or KLINE_LIMIT_1M
    window = list(klines[:window_size] if newest_first else klines[-window_size:])
    high = 0.0
    low = 0.0
    for candle in window:
        _ts, candle_high, candle_low = current_candle_extra(candle)
        if candle_high > 0:
            high = max(high, candle_high) if high > 0 else candle_high
        if candle_low > 0:
            low = min(low, candle_low) if low > 0 else candle_low
    newest = klines[0] if newest_first else klines[-1]
    ts, newest_high, newest_low = current_candle_extra(newest)
    try:
        newest_close = float(newest[4])
    except (TypeError, ValueError, IndexError):
        newest_close = 0.0
    return high, low, ts, newest_high, newest_low, newest_close


def _same_open_ts(left, right) -> bool:
    if left is None or right is None:
        return False
    try:
        left_ts = float(left)
        right_ts = float(right)
    except (TypeError, ValueError):
        return False
    if left_ts > 1e12:
        left_ts /= 1000.0
    if right_ts > 1e12:
        right_ts /= 1000.0
    return abs(left_ts - right_ts) < 1


def _episode_active(density) -> bool:
    value = density.get('bounce_armed')
    if isinstance(value, str):
        return value.strip().lower() in ('1', 'true', 'yes')
    if value is not None:
        return bool(value)
    return bool(density.get('touch_active')) and not bool(density.get('touch_bounced'))


def apply_1m_extremes(density, high, low, candle_ts, bar_high=None, bar_low=None, bar_close=None):
    try:
        high = float(high or 0)
        low = float(low or 0)
        candle_ts = float(candle_ts or 0)
        bar_high = float(bar_high or 0)
        bar_low = float(bar_low or 0)
        bar_close = float(bar_close or 0)
    except (TypeError, ValueError):
        return
    if high <= 0 and low <= 0 and bar_high <= 0 and bar_low <= 0:
        return
    density['watch_1m'] = True
    if high > 0:
        density['candle_1m_high'] = high
        prev = _density_float(density, 'm1_ext_high', 0)
        density['m1_ext_high'] = max(prev, high) if prev > 0 else high
    if low > 0:
        density['candle_1m_low'] = low
        prev = _density_float(density, 'm1_ext_low', 0)
        density['m1_ext_low'] = min(prev, low) if prev > 0 else low
    if bar_high > 0:
        density['candle_1m_bar_high'] = bar_high
    if bar_low > 0:
        density['candle_1m_bar_low'] = bar_low
    if bar_close > 0:
        density['candle_1m_close'] = bar_close
    if candle_ts > 1e12:
        candle_ts /= 1000.0
    if candle_ts > 0:
        density['candle_1m_ts'] = candle_ts
    episode_high = bar_high if bar_high > 0 else high
    episode_low = bar_low if bar_low > 0 else low
    on_touch_bar = _same_open_ts(candle_ts, density.get('touch_candle_ts'))
    if _episode_active(density) and not on_touch_bar:
        if episode_high > 0:
            prev = _density_float(density, 'm1_episode_high', 0)
            density['m1_episode_high'] = max(prev, episode_high) if prev > 0 else episode_high
        if episode_low > 0:
            prev = _density_float(density, 'm1_episode_low', 0)
            density['m1_episode_low'] = min(prev, episode_low) if prev > 0 else episode_low


async def attach_1m_extremes(density, fetch_1m, prefetched=None):
    """1m для подходов и отскока: параллельный снимок или добор в эту же минуту."""
    if not should_watch_1m(density):
        return
    data = prefetched
    if not data or (not data[0] and not data[1]):
        try:
            data = await fetch_1m()
        except Exception:
            return
    if not data:
        return
    high, low, ts = data[0], data[1], data[2] if len(data) > 2 else 0
    bar_high = data[3] if len(data) > 3 else 0
    bar_low = data[4] if len(data) > 4 else 0
    bar_close = data[5] if len(data) > 5 else 0
    apply_1m_extremes(density, high, low, ts, bar_high, bar_low, bar_close)


def _fut_episode_active(density) -> bool:
    value = density.get('fut_bounce_armed')
    if isinstance(value, str):
        return value.strip().lower() in ('1', 'true', 'yes')
    return bool(value)


def apply_fut_1m_extremes(density, high, low, candle_ts, bar_high=None, bar_low=None, bar_close=None):
    try:
        high = float(high or 0)
        low = float(low or 0)
        candle_ts = float(candle_ts or 0)
        bar_high = float(bar_high or 0)
        bar_low = float(bar_low or 0)
        bar_close = float(bar_close or 0)
    except (TypeError, ValueError):
        return
    if high <= 0 and low <= 0 and bar_high <= 0 and bar_low <= 0 and bar_close <= 0:
        return
    if high > 0:
        density['candle_fut_1m_high'] = high
    if low > 0:
        density['candle_fut_1m_low'] = low
    if bar_high > 0:
        density['candle_fut_1m_bar_high'] = bar_high
    if bar_low > 0:
        density['candle_fut_1m_bar_low'] = bar_low
    if bar_close > 0:
        density['candle_fut_1m_close'] = bar_close
    if candle_ts > 1e12:
        candle_ts /= 1000.0
    if candle_ts > 0:
        density['candle_fut_1m_ts'] = candle_ts
    episode_high = bar_high if bar_high > 0 else high
    episode_low = bar_low if bar_low > 0 else low
    on_touch_bar = _same_open_ts(candle_ts, density.get('fut_touch_candle_ts'))
    if _fut_episode_active(density) and not on_touch_bar:
        if episode_high > 0:
            prev = _density_float(density, 'fut_episode_high', 0)
            density['fut_episode_high'] = max(prev, episode_high) if prev > 0 else episode_high
        if episode_low > 0:
            prev = _density_float(density, 'fut_episode_low', 0)
            density['fut_episode_low'] = min(prev, episode_low) if prev > 0 else episode_low


async def attach_fut_1m_extremes(density, exchange, symbol, session):
    """Фьючерсная 1m: точка отсчёта и отскок перпа после спотового подхода."""
    if not should_watch_1m(density) or not session:
        return
    from utils.futures_klines import fetch_futures_1m_extremes
    try:
        data = await fetch_futures_1m_extremes(session, exchange, symbol)
    except Exception:
        return
    if not data:
        return
    apply_fut_1m_extremes(density, *data)


def apply_candle_extremes(density, high, low, candle_ts):
    try:
        high = float(high or 0)
        low = float(low or 0)
        candle_ts = float(candle_ts or 0)
    except (TypeError, ValueError):
        return
    if high > 0:
        density['candle_high'] = high
    if low > 0:
        density['candle_low'] = low
    if candle_ts > 0:
        if candle_ts > 1e12:
            candle_ts /= 1000.0
        density['candle_open_ts'] = candle_ts


def unpack_kline_info(kline_data):
    data = tuple(kline_data or ())
    percent = data[0] if data else 100
    vol_last = data[1] if len(data) > 1 else 0
    vol_cur = data[2] if len(data) > 2 else 0
    avg_vol = data[3] if len(data) > 3 else 0
    volatility = data[4] if len(data) > 4 else 0
    current_price = data[5] if len(data) > 5 else 0
    high = data[6] if len(data) > 6 else 0
    low = data[7] if len(data) > 7 else 0
    candle_ts = data[8] if len(data) > 8 else 0
    return percent, vol_last, vol_cur, avg_vol, volatility, current_price, high, low, candle_ts
