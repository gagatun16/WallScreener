import csv
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from config.params import (
    BOUNCE_MIN_PERCENT,
    BOUNCE_REVERSAL_PCT,
    SPOT_BOUNCE_REVERSAL_PCT,
    BASIS_MAX_ABS_PCT,
    PULL_MAX_PERCENT,
    MIN_LIFE_TIME,
    EATEN_BREAK_RATIO,
    TRADE_LOOKBACK_SEC,
)
from utils.density_filter import (
    _density_float,
    wall_close_1m_percent,
    wall_fut_close_1m_percent,
    wall_wick_1m_percent,
)
from utils.outcome_chart import make_chart_id, save_outcome_chart
from utils.price_step import attach_price_steps, ensure_price_steps, fill_row_price_steps
from utils.price_trades import lookback_start_ts, quote_volume_at_price

OUTCOME_BOUNCE = 'отскок'
OUTCOME_BOUNCE_FUT = 'отскок фьюч'
OUTCOME_TOUCH = 'подход'
OUTCOME_PULL = 'снятие'
OUTCOME_PULL_NO_ATTACK = 'Снятие без атаки'
OUTCOME_BREAK = 'пробой'
OUTCOME_EATEN = 'разъедание'
OUTCOME_JUNK = 'мусор'

BREAK_EPS = 0.0005
TOUCH_AT_WALL = 0.005  # min% 0.00 — смотреть ленту по цене стены

CSV_PATH = Path('data') / 'outcomes.csv'
JOURNAL_TZ = timezone(timedelta(hours=7))
CSV_FIELDS = [
    'closed_at',
    'exchange',
    'side',
    'symbol',
    'wall_id',
    'wall_price',
    'last_price',
    'outcome',
    'winrate_pct',
    'winrate_fut_pct',
    'life_time_min',
    'min_to_first_touch',
    'min_touch_to_bounce',
    'min_touch_to_close',
    'min_first_touch_to_close',
    'min_percent',
    'last_percent',
    'last_fut_percent',
    'max_bounce',
    'spot_adverse_pct',
    'max_fut_bounce',
    'bounce_move',
    'touch_price',
    'touch_percent',
    'bounce_price',
    'touch_count',
    'bounce_count',
    'eaten_ratio',
    'attack_eaten_ratio',
    'remaining_pct',
    'eaten_usdt',
    'order_size_usdt',
    'peak_order_usdt',
    'qty_size',
    'volatility',
    'volume_ratio_avg',
    'volume_ratio_last',
    'volume_ratio_current',
    'found_percent',
    'spot_tick_pct',
    'fut_tick_pct',
    'fut_touch_price',
    'basis_pct',
    'fut_adverse_pct',
    'price_beyond_episode',
    'btc_move_12x5m',
    'btc_touch_12x5m',
    'btc_bounce_nx5m',
    'chart_id',
]

_lock = threading.Lock()
_counts = {
    OUTCOME_BOUNCE: 0,
    OUTCOME_BOUNCE_FUT: 0,
    OUTCOME_TOUCH: 0,
    OUTCOME_PULL: 0,
    OUTCOME_PULL_NO_ATTACK: 0,
    OUTCOME_BREAK: 0,
    OUTCOME_EATEN: 0,
    OUTCOME_JUNK: 0,
}


def density_key(density) -> str:
    return f"{density.get('symbol')}_{density.get('price')}_{density.get('side')}"


def ensure_wall_id(exchange, density) -> str:
    existing = density.get('wall_id')
    if existing:
        return str(existing)
    first = _unix_ts(density.get('first_seen_ts'))
    try:
        price_s = f"{float(density.get('price') or 0):.10g}"
    except (TypeError, ValueError):
        price_s = '0'
    side = 'bid' if is_bid_side(density) else 'ask'
    symbol = str(density.get('symbol') or '')
    exch = str(exchange or density.get('exchange') or '').strip()
    wall_id = f"{exch}_{symbol}_{side}_{price_s}_{int(first) if first else 0}"
    density['wall_id'] = wall_id
    return wall_id


def _minutes_between(start, end):
    start = _unix_ts(start)
    end = _unix_ts(end)
    if start <= 0 or end <= 0:
        return ''
    return round(max(0.0, (end - start) / 60.0), 1)


def is_bid_side(density) -> bool:
    side = str(density.get('side', ''))
    return side in ('ПОКУПКА', 'bid', 'BUY')


def _same_candle_ts(left, right) -> bool:
    if left is None or right is None:
        return False
    try:
        return abs(float(left) - float(right)) < 1
    except (TypeError, ValueError):
        return False


def _unix_ts(raw) -> float:
    try:
        ts = float(raw)
    except (TypeError, ValueError):
        return 0.0
    if ts > 1e12:
        ts /= 1000.0
    return ts if ts > 1e9 else 0.0


def _bar_opened_before_wall(density) -> bool:
    """Текущая 1m открылась до появления стены — её фитиль не подход."""
    first = _unix_ts((density or {}).get('first_seen_ts'))
    opened = _unix_ts((density or {}).get('candle_1m_ts'))
    if first <= 0 or opened <= 0:
        return False
    return opened < first


def _1m_bounce_price(density) -> float:
    """1m-экстремум отскока без отката на 5m (High для BID, Low для ASK)."""
    if is_bid_side(density):
        return _density_float(density, 'm1_ext_high', 0) or _density_float(density, 'candle_1m_high', 0)
    return _density_float(density, 'm1_ext_low', 0) or _density_float(density, 'candle_1m_low', 0)


def bounce_price(density, episode=False, include_touch_bar=False) -> float:
    """Цена отскока: 1m High (BID) / Low (ASK). episode — только ход после текущего подхода."""
    close = _density_float(density, 'current_price', 0)
    if episode:
        high = _density_float(density, 'm1_episode_high', 0)
        low = _density_float(density, 'm1_episode_low', 0)
        if include_touch_bar:
            bar_high = _density_float(density, 'candle_1m_bar_high', 0)
            bar_low = _density_float(density, 'candle_1m_bar_low', 0)
            if bar_high > 0:
                high = max(high, bar_high)
            if bar_low > 0:
                low = min(low, bar_low) if low > 0 else bar_low
        if is_bid_side(density):
            return high if high > 0 else 0.0
        return low if low > 0 else 0.0
    one_m = _1m_bounce_price(density)
    if one_m > 0:
        return one_m
    high = _density_float(density, 'candle_high', 0)
    low = _density_float(density, 'candle_low', 0)
    if is_bid_side(density):
        return high if high > 0 else close
    return low if low > 0 else close


def bounce_from_touch(density, min_pct=None, episode=False, include_touch_bar=False) -> float:
    """Уход от цены min% в сторону отскока, %."""
    wall = _density_float(density, 'price', 0)
    price = bounce_price(density, episode=episode, include_touch_bar=include_touch_bar)
    close = _density_float(density, 'current_price', 0)
    if min_pct is None:
        min_pct = _density_float(density, 'min_percent', _density_float(density, 'percent', 100))
    if wall <= 0 or price <= 0:
        return 0.0

    bid = is_bid_side(density)
    if bid and close > 0 and close < wall * (1 - BREAK_EPS):
        return 0.0
    if (not bid) and close > 0 and close > wall * (1 + BREAK_EPS):
        return 0.0

    if bid:
        touch_price = wall * (1 + min_pct / 100.0)
        if touch_price <= 0:
            return 0.0
        return (price / touch_price - 1.0) * 100.0

    touch_price = wall * (1 - min_pct / 100.0)
    if touch_price <= 0:
        return 0.0
    return (touch_price / price - 1.0) * 100.0


def _is_first_spot_approach(density) -> bool:
    """Ещё действует первый подход (касание стены 0.00%)."""
    return _as_int(density, 'touch_count') <= 1


def spot_bounce_from_touch(density) -> float:
    """Первый отскок — от стены; следующие — от цены этого касания."""
    if _is_first_spot_approach(density):
        return bounce_from_touch(density, min_pct=0.0, episode=True)
    return bounce_from_touch(density, episode=True)


def spot_bounce_threshold(density) -> float:
    """1.1% от стены после первого касания, 0.8% от касания на следующих подходах."""
    if _is_first_spot_approach(density):
        return SPOT_BOUNCE_REVERSAL_PCT
    return BOUNCE_REVERSAL_PCT


def _as_int(density, key, default=0) -> int:
    try:
        return int(float(density.get(key, default) or default))
    except (TypeError, ValueError):
        return default


def _as_bool(density, key) -> bool:
    value = density.get(key)
    if isinstance(value, str):
        return value.strip().lower() in ('1', 'true', 'yes')
    return bool(value)


def _touch_wick_price(density) -> float:
    """Цена подхода: BID → Low 1m, ASK → High 1m."""
    if is_bid_side(density):
        return _density_float(density, 'candle_1m_bar_low', 0)
    return _density_float(density, 'candle_1m_bar_high', 0)


def _has_first_touch(density) -> bool:
    """Первый подход к стене (min%=0.00) уже был."""
    if _as_int(density, 'touch_count') > 0:
        return True
    return _unix_ts(density.get('first_touch_ts')) > 0


def wick_percent(density) -> float:
    """Расстояние до стены по фитилю текущей 1m: BID → Low, ASK → High."""
    wick = wall_wick_1m_percent(density)
    return 100.0 if wick is None else wick


def update_touch_episodes(density, closing=False) -> None:
    """Первый подход — только min%=0.00. После него плотность настоящая, дальше зона 0.3%."""
    density.setdefault('touch_count', 0)
    density.setdefault('bounce_count', 0)
    if 'percent' not in density:
        return False

    wick = wall_wick_1m_percent(density)
    close_pct = wall_close_1m_percent(density)
    survived = _as_bool(density, 'survived_attack')
    approach_zone = BOUNCE_MIN_PERCENT if survived else TOUCH_AT_WALL
    in_zone = (
        wick is not None
        and wick < approach_zone
        and close_pct is not None
        and close_pct < PULL_MAX_PERCENT
        and not _bar_opened_before_wall(density)
    )
    touch_count = _as_int(density, 'touch_count')
    bounce_count = _as_int(density, 'bounce_count')
    if 'bounce_armed' in density:
        bounce_armed = _as_bool(density, 'bounce_armed')
    else:
        bounce_armed = _as_bool(density, 'touch_active') and not _as_bool(density, 'touch_bounced')
    candle_ts = density.get('candle_1m_ts')
    was_in_zone = _as_bool(density, 'touch_in_zone')
    on_touch_bar = _same_candle_ts(candle_ts, density.get('touch_candle_ts'))
    new_touch = False
    bounced = False
    # Вышли из зоны и снова коснулись: это новый подход, не отскок от старого min%.
    reentering = in_zone and not was_in_zone

    if bounce_armed and not on_touch_bar and not reentering:
        move = spot_bounce_from_touch(density)
        if move >= spot_bounce_threshold(density):
            _snapshot_spot_adverse(density)
            _reset_spot_adverse(density)
            bounce_count += 1
            bounce_armed = False
            bounced = True

    # На закрытии стены не начинаем новый подход — иначе обнуляются min%/max%/против.
    if reentering and not closing:
        new_touch = True
        if candle_ts:
            try:
                density['touch_candle_ts'] = float(candle_ts)
            except (TypeError, ValueError):
                density['touch_candle_ts'] = candle_ts

    if new_touch:
        touch_count += 1
        bounce_armed = True
        density.pop('m1_episode_high', None)
        density.pop('m1_episode_low', None)
        _reset_episode_peaks(density)
        if wick is not None:
            density['min_percent'] = round(wick, 2)
        touch_price = _touch_wick_price(density)
        if touch_price > 0:
            density['last_touch_price'] = touch_price
        if wick is not None:
            density['last_touch_percent'] = round(wick, 2)
        if wick is not None and wick < TOUCH_AT_WALL:
            density['touch_eaten_pending'] = True
            density['survived_attack'] = True
        touch_ts = _unix_ts(density.get('touch_candle_ts')) or time.time()
        if _unix_ts(density.get('first_touch_ts')) <= 0:
            density['first_touch_ts'] = touch_ts
        density['last_touch_ts'] = touch_ts

    density['touch_count'] = touch_count
    density['bounce_count'] = bounce_count
    density['bounce_armed'] = bounce_armed
    density['touch_active'] = bounce_armed
    density['touch_bounced'] = not bounce_armed
    density['touch_in_zone'] = in_zone
    density['new_touch'] = new_touch
    if wick is not None and wick < BOUNCE_MIN_PERCENT:
        density['watch_1m'] = True
    return bounced


def fut_bounce_from_touch(density) -> float:
    """Уход фьючерсной 1m от close в момент спотового подхода, %."""
    touch = _density_float(density, 'fut_touch_price', 0)
    if touch <= 0:
        return 0.0
    if is_bid_side(density):
        price = _density_float(density, 'fut_episode_high', 0)
        if price <= 0:
            return 0.0
        return (price / touch - 1.0) * 100.0
    price = _density_float(density, 'fut_episode_low', 0)
    if price <= 0:
        return 0.0
    return (touch / price - 1.0) * 100.0


def _fut_bounce_price(density) -> float:
    if is_bid_side(density):
        return _density_float(density, 'fut_episode_high', 0)
    return _density_float(density, 'fut_episode_low', 0)


def _reset_episode_peaks(density) -> None:
    """Свой max%/против Ф/BTC на каждый подход, а не пик за жизнь стены."""
    density['max_bounce'] = 0.0
    density.pop('max_bounce_price', None)
    density['max_fut_bounce'] = 0.0
    _reset_fut_adverse(density)
    _reset_spot_adverse(density)
    density.pop('episode_btc_touch', None)
    density['price_beyond_episode'] = False


def _arm_fut_from_close(density, fut_close: float) -> None:
    density['fut_touch_price'] = fut_close
    density['fut_touch_pending'] = False
    density['fut_bounce_armed'] = True
    density.pop('fut_episode_high', None)
    density.pop('fut_episode_low', None)
    density['max_fut_bounce'] = 0.0
    _reset_fut_adverse(density)
    fut_ts = density.get('candle_fut_1m_ts')
    if fut_ts:
        try:
            density['fut_touch_candle_ts'] = float(fut_ts)
        except (TypeError, ValueError):
            density['fut_touch_candle_ts'] = fut_ts


def fut_adverse_from_entry(density) -> tuple[float, float]:
    """Ход фьюча против отскока от цены входа: BID ниже входа, ASK выше. (% , цена)."""
    entry = _density_float(density, 'fut_touch_price', 0)
    if entry <= 0:
        return 0.0, 0.0
    if is_bid_side(density):
        lows = [
            _density_float(density, 'fut_episode_low', 0),
            _density_float(density, 'candle_fut_1m_bar_low', 0),
        ]
        extreme = min((price for price in lows if price > 0), default=0.0)
        if extreme <= 0 or extreme >= entry:
            return 0.0, 0.0
        return (entry - extreme) / entry * 100.0, extreme
    highs = [
        _density_float(density, 'fut_episode_high', 0),
        _density_float(density, 'candle_fut_1m_bar_high', 0),
    ]
    extreme = max((price for price in highs if price > 0), default=0.0)
    if extreme <= entry:
        return 0.0, 0.0
    return (extreme - entry) / entry * 100.0, extreme


def _reset_fut_adverse(density) -> None:
    density['fut_adverse_pct'] = 0.0
    density['fut_adverse_price'] = 0.0


def _reset_spot_adverse(density) -> None:
    density['spot_adverse_pct'] = 0.0
    density['spot_adverse_price'] = 0.0


def _snapshot_spot_adverse(density) -> None:
    pct = _density_float(density, 'spot_adverse_pct', 0)
    price = _density_float(density, 'spot_adverse_price', 0)
    density['last_spot_adverse_pct'] = round(pct, 2) if pct > 0 else 0.0
    if price > 0:
        density['last_spot_adverse_price'] = price


def spot_adverse_from_entry(density) -> tuple[float, float]:
    """Ход спота против отскока от вход спот: BID ниже входа, ASK выше."""
    entry = _density_float(density, 'last_touch_price', 0)
    if entry <= 0:
        return 0.0, 0.0
    if is_bid_side(density):
        lows = [
            _density_float(density, 'm1_episode_low', 0),
            _density_float(density, 'candle_1m_bar_low', 0),
        ]
        extreme = min((price for price in lows if price > 0), default=0.0)
        if extreme <= 0 or extreme >= entry:
            return 0.0, 0.0
        return (entry - extreme) / entry * 100.0, extreme
    highs = [
        _density_float(density, 'm1_episode_high', 0),
        _density_float(density, 'candle_1m_bar_high', 0),
    ]
    extreme = max((price for price in highs if price > 0), default=0.0)
    if extreme <= entry:
        return 0.0, 0.0
    return (extreme - entry) / entry * 100.0, extreme


def update_spot_adverse_peak(density, closing=False) -> None:
    """Максимум против входа на споте, пока ждём отскок после подхода."""
    if _density_float(density, 'last_touch_price', 0) <= 0:
        return
    if not closing and not _as_bool(density, 'bounce_armed'):
        return
    move, price = spot_adverse_from_entry(density)
    if closing:
        move = max(
            move,
            _density_float(density, 'spot_adverse_pct', 0),
            _density_float(density, 'last_spot_adverse_pct', 0),
        )
    prev = _density_float(density, 'spot_adverse_pct', 0)
    if move > prev:
        density['spot_adverse_pct'] = round(move, 2)
        if price > 0:
            density['spot_adverse_price'] = price


def _spot_adverse_for_row(density, outcome: str):
    current = density.get('spot_adverse_pct')
    stored = density.get('last_spot_adverse_pct')
    if outcome == OUTCOME_BOUNCE and stored not in (None, ''):
        return _csv_optional(stored, 2)
    if outcome in (OUTCOME_PULL, OUTCOME_BREAK):
        try:
            return _csv_optional(max(float(current or 0), float(stored or 0)), 2)
        except (TypeError, ValueError):
            pass
    return _csv_optional(current, 2)


def _snapshot_fut_adverse(density) -> None:
    pct = _density_float(density, 'fut_adverse_pct', 0)
    price = _density_float(density, 'fut_adverse_price', 0)
    density['last_fut_adverse_pct'] = round(pct, 2) if pct > 0 else 0.0
    if price > 0:
        density['last_fut_adverse_price'] = price


def update_fut_adverse_peak(density, closing=False) -> None:
    """Максимум против входа, пока ждём отскок на фьючерсах после подхода."""
    if _density_float(density, 'fut_touch_price', 0) <= 0:
        return
    if not closing and not _as_bool(density, 'fut_bounce_armed'):
        return
    move, price = fut_adverse_from_entry(density)
    if closing:
        move = max(
            move,
            _density_float(density, 'fut_adverse_pct', 0),
            _density_float(density, 'last_fut_adverse_pct', 0),
        )
    prev = _density_float(density, 'fut_adverse_pct', 0)
    if move > prev:
        density['fut_adverse_pct'] = round(move, 2)
        if price > 0:
            density['fut_adverse_price'] = price


def _fut_adverse_for_row(density, outcome: str):
    current = density.get('fut_adverse_pct')
    stored = density.get('last_fut_adverse_pct')
    if outcome == OUTCOME_BOUNCE_FUT and stored not in (None, ''):
        return _csv_optional(stored, 2)
    if outcome in (OUTCOME_PULL, OUTCOME_BREAK):
        try:
            return _csv_optional(max(float(current or 0), float(stored or 0)), 2)
        except (TypeError, ValueError):
            pass
    return _csv_optional(current, 2)


def update_fut_episodes(density, closing=False) -> bool:
    """После спотового подхода замораживает фьюч close и ждёт ход 0.8% со следующей 1m."""
    density.setdefault('fut_bounce_count', 0)
    new_touch = _as_bool(density, 'new_touch')
    pending = _as_bool(density, 'fut_touch_pending')
    armed = _as_bool(density, 'fut_bounce_armed')
    fut_close = _density_float(density, 'candle_fut_1m_close', 0)
    on_touch_bar = _same_candle_ts(
        density.get('candle_fut_1m_ts'), density.get('fut_touch_candle_ts'),
    )
    bounced = False

    if new_touch and not closing:
        if fut_close > 0:
            _arm_fut_from_close(density, fut_close)
            armed = True
            on_touch_bar = True
        else:
            density['fut_touch_pending'] = True
            density['fut_bounce_armed'] = False
            armed = False
    elif pending and fut_close > 0 and not closing:
        _arm_fut_from_close(density, fut_close)
        armed = True
        on_touch_bar = True

    if armed and not on_touch_bar:
        update_fut_adverse_peak(density)
        move = fut_bounce_from_touch(density)
        if move >= BOUNCE_REVERSAL_PCT:
            _snapshot_fut_adverse(density)
            _reset_fut_adverse(density)
            density['fut_bounce_count'] = _as_int(density, 'fut_bounce_count') + 1
            density['fut_bounce_armed'] = False
            density['fut_bounce_just_happened'] = True
            bounced = True
            density['last_fut_bounce_move'] = round(move, 2)
            extreme = _fut_bounce_price(density)
            if extreme > 0:
                density['last_fut_bounce_price'] = extreme
            delay = _minutes_between(density.get('last_touch_ts'), time.time())
            density['last_fut_bounce_delay_min'] = delay
            delays = density.get('fut_bounce_delays')
            if not isinstance(delays, list):
                delays = []
            delays.append(delay)
            density['fut_bounce_delays'] = delays
            moves = density.get('fut_bounce_moves')
            if not isinstance(moves, list):
                moves = []
            moves.append(round(move, 2))
            density['fut_bounce_moves'] = moves
            prices = density.get('fut_bounce_prices')
            if not isinstance(prices, list):
                prices = []
            prices.append(extreme if extreme > 0 else None)
            density['fut_bounce_prices'] = prices

    density.pop('new_touch', None)
    return bounced


def _sync_wall_peak(density) -> None:
    """Пик = то, что сейчас в стакане, плюс уже съеденное. Снятие заявки пик уменьшает, сделки — нет."""
    current = _density_float(density, 'order_size', 0)
    eaten = _density_float(density, 'eaten_traded_usdt', 0)
    if current <= 0 and eaten <= 0:
        if density.get('max_order_size') is None:
            density['max_order_size'] = 0.0
        return
    density['max_order_size'] = current + eaten


def _wall_size_usdt(density) -> float:
    return max(
        _density_float(density, 'max_order_size', 0),
        _density_float(density, 'order_size', 0),
    )


def _stored_eaten_ratio(density):
    raw = density.get('eaten_ratio')
    if raw in (None, ''):
        return None
    try:
        return float(raw)
    except (TypeError, ValueError):
        return None


def _recompute_eaten(density) -> float | None:
    """Разбор = съеденное / актуальный пик. Пик падает, если заявку сняли, и растёт, если доложили."""
    if 'eaten_traded_usdt' not in density and density.get('eaten_ratio') in (None, ''):
        return None
    size = _wall_size_usdt(density)
    if size <= 0:
        return _stored_eaten_ratio(density)
    ratio = _density_float(density, 'eaten_traded_usdt', 0) / size
    density['eaten_ratio'] = round(ratio, 4)
    density['remaining_pct'] = round(max(0.0, (1.0 - ratio) * 100.0), 2)
    return ratio


def _commit_eaten(density, extra_usdt: float, now: float) -> float:
    """Добавить проторгованный USDT. Сумма сделок только растёт, доля — от текущего пика стены."""
    total = _density_float(density, 'eaten_traded_usdt', 0) + max(0.0, extra_usdt)
    density['eaten_traded_usdt'] = total
    density['last_eaten_ts'] = now
    return _recompute_eaten(density) or 0.0


def _eaten_lookback_start(density, now: float) -> float:
    start = lookback_start_ts(density, now)
    start = max(start, now - TRADE_LOOKBACK_SEC)
    first = _unix_ts(density.get('first_seen_ts'))
    if first > 0:
        start = max(start, first)
    return start


async def apply_touch_eaten(exchange: str, density: dict) -> float | None:
    """При подходе min%=0.00: сделки по цене стены → разбор и остаток плотности."""
    if not _as_bool(density, 'touch_eaten_pending'):
        return None
    wall = _density_float(density, 'price', 0)
    size = _wall_size_usdt(density)
    symbol = density.get('symbol')
    if wall <= 0 or size <= 0 or not symbol:
        density['touch_eaten_pending'] = False
        return None

    now = time.time()
    start = lookback_start_ts(density, now)
    opened = _unix_ts(density.get('touch_candle_ts'))
    if opened > 0:
        start = min(start, opened)
    last_eaten = _unix_ts(density.get('last_eaten_ts'))
    if last_eaten > 0:
        start = max(start, last_eaten + 0.001)
    start = max(start, now - TRADE_LOOKBACK_SEC)
    first = _unix_ts(density.get('first_seen_ts'))
    if first > 0:
        start = max(start, first)

    traded = await quote_volume_at_price(exchange, symbol, wall, start, now)
    if traded is None:
        return None

    density['touch_eaten_pending'] = False
    density['last_touch_traded_usdt'] = traded
    density['last_touch_eaten_ratio'] = round(traded / size, 4)
    ratio = _commit_eaten(density, traded, now)
    if traded > 0:
        density['eaten_attack_count'] = _as_int(density, 'eaten_attack_count') + 1
    print(
        f"📐 {exchange}: {density.get('side')} {symbol} стена={wall} "
        f"разбор={density['eaten_ratio']} осталось={density['remaining_pct']}% "
        f"атака={density['last_touch_eaten_ratio']}"
    )
    return ratio


async def finalize_eaten_on_close(exchange: str, density: dict) -> float | None:
    """При закрытии добрать сделки после последнего замера, не пересчитывая разбор заново."""
    stored = _stored_eaten_ratio(density)
    wall = _density_float(density, 'price', 0)
    size = _wall_size_usdt(density)
    symbol = density.get('symbol')
    if wall <= 0 or size <= 0 or not symbol:
        return stored

    now = time.time()
    last_eaten = _unix_ts(density.get('last_eaten_ts'))
    if last_eaten > 0:
        start = last_eaten + 0.001
    else:
        start = _eaten_lookback_start(density, now)
    if start >= now:
        return _recompute_eaten(density) if stored is not None else stored

    traded = await quote_volume_at_price(exchange, symbol, wall, start, now)
    if traded is None:
        return _recompute_eaten(density) if stored is not None else stored
    return _commit_eaten(density, traded, now)


async def record_eaten_attack_outcome(exchange: str, density: dict) -> str | None:
    """Строка «разъедание» на каждый подход 0.00% со сделками по цене стены."""
    if not has_tracked_snapshot(density):
        return None
    count = _as_int(density, 'eaten_attack_count')
    written = _as_int(density, 'eaten_attack_rows_written')
    if count <= written:
        return None
    last_written = None
    while written < count:
        last_written = await _write_journal_row(
            exchange, density, OUTCOME_EATEN, _stored_eaten_ratio(density),
        )
        written += 1
        density['eaten_attack_rows_written'] = written
    return last_written


async def record_touch_outcome(exchange: str, density: dict) -> str | None:
    """Строка «подход», если к стене подошли и это не разъедание."""
    if not has_tracked_snapshot(density):
        return None
    min_pct = _density_float(density, 'min_percent', _density_float(density, 'percent', 100))
    if min_pct >= BOUNCE_MIN_PERCENT:
        return None
    should = max(0, _as_int(density, 'touch_count') - _as_int(density, 'eaten_attack_count'))
    written = _as_int(density, 'touch_rows_written')
    if should <= written:
        return None
    last_written = None
    while written < should:
        last_written = await _write_journal_row(
            exchange, density, OUTCOME_TOUCH, _stored_eaten_ratio(density),
        )
        written += 1
        density['touch_rows_written'] = written
    return last_written


def update_peaks(density, closing=False) -> None:
    if density.get('first_seen_ts') is None:
        life_time = _density_float(density, 'life_time', 0)
        if life_time > 1e9:
            density['first_seen_ts'] = life_time
        elif life_time > 0:
            density['first_seen_ts'] = time.time() - life_time
        else:
            density['first_seen_ts'] = time.time()
    ensure_wall_id(density.get('exchange'), density)
    attach_price_steps(density)

    if 'percent' in density:
        wick = wall_wick_1m_percent(density)
        close_pct = wall_close_1m_percent(density)
        valid_1m = (
            wick is not None
            and close_pct is not None
            and close_pct < PULL_MAX_PERCENT
            and not _bar_opened_before_wall(density)
        )
        if valid_1m:
            previous = density.get('min_percent')
            if previous is None:
                density['min_percent'] = round(wick, 2)
            else:
                density['min_percent'] = round(min(_density_float(density, 'min_percent', wick), wick), 2)
        elif density.get('min_percent') is None:
            density['min_percent'] = 100.0
        if _density_float(density, 'min_percent', 100) < BOUNCE_MIN_PERCENT:
            density['watch_1m'] = True
        if density.get('found_percent') is None and 'percent' in density:
            density['found_percent'] = round(_density_float(density, 'percent', 100), 2)

    update_fut_adverse_peak(density, closing=closing)
    update_spot_adverse_peak(density, closing=closing)
    bounced = update_touch_episodes(density, closing=closing)
    update_fut_episodes(density, closing=closing)
    update_fut_adverse_peak(density, closing=closing)
    update_spot_adverse_peak(density, closing=closing)
    if bounced:
        density['bounce_just_happened'] = True
        move = round(max(0.0, spot_bounce_from_touch(density)), 2)
        extreme = bounce_price(density, episode=True)
        density['last_bounce_move'] = move
        if extreme > 0:
            density['last_bounce_price'] = extreme
        moves = density.get('bounce_moves')
        if not isinstance(moves, list):
            moves = []
        moves.append(move)
        density['bounce_moves'] = moves
        prices = density.get('bounce_prices')
        if not isinstance(prices, list):
            prices = []
        prices.append(extreme if extreme > 0 else None)
        density['bounce_prices'] = prices
        delay = _minutes_between(density.get('last_touch_ts'), time.time())
        density['last_bounce_delay_min'] = delay
        delays = density.get('bounce_delays')
        if not isinstance(delays, list):
            delays = []
        delays.append(delay)
        density['bounce_delays'] = delays

    approached = _has_first_touch(density)
    previous_bounce = density.get('max_bounce')
    if not approached:
        density['max_bounce'] = 0.0
        density.pop('max_bounce_price', None)
        density['max_bounce_armed'] = False
    else:
        if not _as_bool(density, 'max_bounce_armed'):
            previous_bounce = 0.0
            density['max_bounce'] = 0.0
            density.pop('max_bounce_price', None)
            density['max_bounce_armed'] = True
        bounce = bounce_from_touch(density, episode=True, include_touch_bar=closing)
        new_max = bounce if previous_bounce in (None, '') else max(
            _density_float(density, 'max_bounce', 0), bounce, 0.0,
        )
        new_max = round(max(0.0, new_max), 2)
        old_max = None if previous_bounce in (None, '') else round(
            _density_float(density, 'max_bounce', 0), 2,
        )
        density['max_bounce'] = new_max
        if old_max is None or new_max > old_max:
            extreme = bounce_price(density, episode=True, include_touch_bar=closing)
            if extreme > 0:
                density['max_bounce_price'] = extreme

    _update_max_fut_bounce(density, closing=closing)
    _stamp_journal_last_percents(density)

    order_size = _density_float(density, 'order_size', 0)
    peak = density.get('peak_order_size')
    if peak is None:
        density['peak_order_size'] = order_size
    else:
        density['peak_order_size'] = max(_density_float(density, 'peak_order_size', 0), order_size)
    _sync_wall_peak(density)
    _recompute_eaten(density)
    stamp_price_beyond_wall(density)

    return bool(bounced)


def _fut_favorable_move_pct(density, include_touch_bar=False) -> float:
    """Ход фьюча в сторону отскока от входа. Свеча подхода не входит, кроме закрытия стены."""
    touch = _density_float(density, 'fut_touch_price', 0)
    if touch <= 0:
        return 0.0
    on_touch_bar = _same_candle_ts(
        density.get('candle_fut_1m_ts'), density.get('fut_touch_candle_ts'),
    )
    if is_bid_side(density):
        highs = [_density_float(density, 'fut_episode_high', 0)]
        if include_touch_bar or not on_touch_bar:
            highs.append(_density_float(density, 'candle_fut_1m_bar_high', 0))
        price = max((value for value in highs if value > 0), default=0.0)
        if price <= 0:
            return 0.0
        return (price / touch - 1.0) * 100.0
    lows = [_density_float(density, 'fut_episode_low', 0)]
    if include_touch_bar or not on_touch_bar:
        lows.append(_density_float(density, 'candle_fut_1m_bar_low', 0))
    price = min((value for value in lows if value > 0), default=0.0)
    if price <= 0:
        return 0.0
    return (touch / price - 1.0) * 100.0


def _update_max_fut_bounce(density, closing=False) -> None:
    """Макс. ход фьюча в сторону отскока после подхода, % от fut_touch_price."""
    if not _has_first_touch(density):
        density['max_fut_bounce'] = 0.0
        density['max_fut_bounce_armed'] = False
        return
    if _density_float(density, 'fut_touch_price', 0) <= 0:
        return
    if not _as_bool(density, 'max_fut_bounce_armed'):
        density['max_fut_bounce'] = 0.0
        density['max_fut_bounce_armed'] = True
    peak = max(
        _fut_favorable_move_pct(density, include_touch_bar=closing),
        _density_float(density, 'max_fut_bounce', 0),
        0.0,
    )
    density['max_fut_bounce'] = round(peak, 2)


def _stamp_journal_last_percents(density) -> None:
    close_pct = wall_close_1m_percent(density)
    if close_pct is not None:
        density['journal_last_percent'] = round(close_pct, 2)
    fut_pct = wall_fut_close_1m_percent(density)
    if fut_pct is not None:
        density['journal_last_fut_percent'] = round(fut_pct, 2)


def _last_percent_for_row(density):
    close_pct = wall_close_1m_percent(density)
    if close_pct is not None:
        return round(close_pct, 2)
    stored = density.get('journal_last_percent')
    if stored not in (None, ''):
        return _csv_optional(stored, 2)
    if 'percent' in density:
        return round(_density_float(density, 'percent', 100), 2)
    return ''


def _last_fut_percent_for_row(density):
    fut_pct = wall_fut_close_1m_percent(density)
    if fut_pct is not None:
        return round(fut_pct, 2)
    return _csv_optional(density.get('journal_last_fut_percent'), 2)


def _beyond_wall_pct(density, fut=False) -> float:
    """На сколько % цена ушла за стену. BID — ниже, ASK — выше."""
    wall = _density_float(density, 'price', 0)
    if wall <= 0:
        return 0.0
    if fut:
        if is_bid_side(density):
            prices = (
                _density_float(density, 'fut_episode_low', 0),
                _density_float(density, 'candle_fut_1m_bar_low', 0),
                _density_float(density, 'candle_fut_1m_close', 0),
            )
            extreme = min((price for price in prices if price > 0), default=0.0)
            if extreme <= 0 or extreme >= wall:
                return 0.0
            return (wall - extreme) / wall * 100.0
        prices = (
            _density_float(density, 'fut_episode_high', 0),
            _density_float(density, 'candle_fut_1m_bar_high', 0),
            _density_float(density, 'candle_fut_1m_close', 0),
        )
        extreme = max((price for price in prices if price > 0), default=0.0)
        if extreme <= wall:
            return 0.0
        return (extreme - wall) / wall * 100.0
    if is_bid_side(density):
        prices = (
            _density_float(density, 'm1_episode_low', 0),
            _density_float(density, 'candle_1m_bar_low', 0),
            _density_float(density, 'candle_1m_close', 0),
            _density_float(density, 'current_price', 0),
        )
        extreme = min((price for price in prices if price > 0), default=0.0)
        if extreme <= 0 or extreme >= wall:
            return 0.0
        return (wall - extreme) / wall * 100.0
    prices = (
        _density_float(density, 'm1_episode_high', 0),
        _density_float(density, 'candle_1m_bar_high', 0),
        _density_float(density, 'candle_1m_close', 0),
        _density_float(density, 'current_price', 0),
    )
    extreme = max((price for price in prices if price > 0), default=0.0)
    if extreme <= wall:
        return 0.0
    return (extreme - wall) / wall * 100.0


def _beyond_from_row_prices(row) -> float:
    wall = 0.0
    try:
        wall = float(row.get('wall_price') or 0)
    except (TypeError, ValueError):
        return 0.0
    if wall <= 0:
        return 0.0
    raw = row.get('last_price')
    try:
        price = float(raw or 0)
    except (TypeError, ValueError):
        price = 0.0
    side = str(row.get('side') or '')
    bid = side == 'ПОКУПКА' or str(side).lower() == 'bid'
    if price <= 0:
        return 0.0
    if bid and price < wall:
        return (wall - price) / wall * 100.0
    if (not bid) and price > wall:
        return (price - wall) / wall * 100.0
    return 0.0


def _winrate_value(outcome, bounce, last, beyond):
    if outcome in (OUTCOME_TOUCH, OUTCOME_EATEN):
        return '—'
    if outcome in (OUTCOME_BOUNCE, OUTCOME_BOUNCE_FUT):
        return _csv_optional(bounce, 2)
    if outcome == OUTCOME_PULL or outcome == OUTCOME_PULL_NO_ATTACK:
        return _csv_optional(last, 2)
    if outcome == OUTCOME_BREAK:
        try:
            extra = float(beyond or 0)
        except (TypeError, ValueError):
            extra = 0.0
        if extra > 0:
            return round(extra, 2)
        return _csv_optional(last, 2)
    return '—'


def _winrate_pair_from_density(density, outcome):
    bounce = density.get('max_bounce')
    bounce_fut = density.get('max_fut_bounce')
    last = _last_percent_for_row(density)
    last_fut = _last_fut_percent_for_row(density)
    beyond = _beyond_wall_pct(density, fut=False)
    beyond_fut = _beyond_wall_pct(density, fut=True)
    return (
        _winrate_value(outcome, bounce, last, beyond),
        _winrate_value(outcome, bounce_fut, last_fut, beyond_fut),
    )


def fill_row_winrate(row: dict) -> None:
    if row.get('winrate_pct') not in (None, '') and row.get('winrate_fut_pct') not in (None, ''):
        return
    outcome = row.get('outcome', '')
    bounce = row.get('max_bounce')
    bounce_fut = row.get('max_fut_bounce')
    last = row.get('last_percent')
    last_fut = row.get('last_fut_percent')
    beyond = _beyond_from_row_prices(row)
    if row.get('winrate_pct') in (None, ''):
        row['winrate_pct'] = _winrate_value(outcome, bounce, last, beyond)
    if row.get('winrate_fut_pct') in (None, ''):
        row['winrate_fut_pct'] = _winrate_value(outcome, bounce_fut, last_fut, last_fut)


def _min_percent_for_row(density):
    value = density.get('min_percent')
    if value not in (None, ''):
        try:
            number = float(value)
            if number < 100:
                return round(number, 2)
        except (TypeError, ValueError):
            pass
    touch = density.get('last_touch_percent')
    if touch not in (None, ''):
        return _csv_optional(touch, 2)
    return round(_density_float(density, 'percent', 100), 2)


def has_tracked_snapshot(density) -> bool:
    """В журнал только плотности, у которых трекер хотя бы раз записал цену."""
    if _density_float(density, 'current_price', 0) <= 0:
        return False
    return 'percent' in density


def stamp_last_checked(density) -> None:
    density['last_checked_ts'] = time.time()


def stamp_price_beyond_wall(density) -> None:
    """Если цена уже за стеной — запомнить, даже если потом вернётся."""
    if _as_bool(density, 'price_beyond_wall') or _price_beyond_wall_now(density):
        density['price_beyond_wall'] = True
    if not _has_first_touch(density):
        density['price_beyond_episode'] = False
        return
    if _as_bool(density, 'price_beyond_episode') or _price_beyond_episode_now(density):
        density['price_beyond_episode'] = True


def _price_beyond_wall_now(density) -> bool:
    """Покупка: цена ниже стены на 0.05%. Продажа: выше на 0.05%."""
    return _price_beyond_from_keys(
        density,
        (
            'current_price', 'candle_low', 'candle_1m_low',
            'candle_1m_bar_low', 'm1_ext_low', 'm1_episode_low',
        ),
        (
            'current_price', 'candle_high', 'candle_1m_high',
            'candle_1m_bar_high', 'm1_ext_high', 'm1_episode_high',
        ),
    )


def _price_beyond_episode_now(density) -> bool:
    """За стеной только по текущему подходу: close и 1m этого эпизода, без окна за жизнь стены."""
    return _price_beyond_from_keys(
        density,
        ('current_price', 'candle_1m_bar_low', 'm1_episode_low'),
        ('current_price', 'candle_1m_bar_high', 'm1_episode_high'),
    )


def _price_beyond_from_keys(density, bid_keys, ask_keys) -> bool:
    wall = _density_float(density, 'price', 0)
    if wall <= 0:
        return False
    if is_bid_side(density):
        limit = wall * (1.0 - BREAK_EPS)
        for key in bid_keys:
            price = _density_float(density, key, 0)
            if price > 0 and price < limit:
                return True
        return False
    limit = wall * (1.0 + BREAK_EPS)
    for key in ask_keys:
        price = _density_float(density, key, 0)
        if price > 0 and price > limit:
            return True
    return False


def price_crossed_wall(density) -> bool:
    return _as_bool(density, 'price_beyond_wall') or _price_beyond_wall_now(density)


def classify_outcome(density) -> str:
    min_pct = _density_float(density, 'min_percent', _density_float(density, 'percent', 100))
    eaten = density.get('eaten_ratio')
    if eaten is not None:
        try:
            eaten = float(eaten)
        except (TypeError, ValueError):
            eaten = None

    if (
        eaten is not None
        and eaten >= EATEN_BREAK_RATIO
        and price_crossed_wall(density)
    ):
        return OUTCOME_BREAK

    if not _has_first_touch(density):
        return OUTCOME_PULL_NO_ATTACK
    if min_pct < PULL_MAX_PERCENT:
        return OUTCOME_PULL
    return OUTCOME_JUNK


def _life_time_sec(density: dict) -> float:
    life_time = _density_float(density, 'life_time', 0)
    first_seen = density.get('first_seen_ts') or density.get('bid_first_seen') or density.get('ask_first_seen')
    if life_time > 1e9 or life_time <= 0:
        if first_seen:
            try:
                return max(0.0, time.time() - float(first_seen))
            except (TypeError, ValueError):
                return 0.0
        return 0.0
    return life_time


def _ensure_csv_header_locked() -> None:
    if not CSV_PATH.exists():
        return
    with CSV_PATH.open('r', encoding='utf-8-sig', newline='') as file:
        reader = csv.DictReader(file)
        fieldnames = list(reader.fieldnames or [])
        rows = list(reader)
    if fieldnames == list(CSV_FIELDS):
        return
    with CSV_PATH.open('w', encoding='utf-8-sig', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=CSV_FIELDS, extrasaction='ignore')
        writer.writeheader()
        for row in rows:
            packed = {key: row.get(key, '') for key in CSV_FIELDS}
            packed['life_time_min'] = _life_time_min_from_row(row)
            writer.writerow(packed)


def _basis_pct_from_prices(spot, fut):
    """(вход фьюч / вход спот − 1) × 100. Пусто, если нет одной из цен."""
    try:
        spot = float(spot)
        fut = float(fut)
    except (TypeError, ValueError):
        return ''
    if spot <= 0 or fut <= 0:
        return ''
    return round((fut / spot - 1.0) * 100.0, 2)


def fill_row_basis_pct(row: dict) -> None:
    if row.get('basis_pct') not in (None, ''):
        return
    filled = _basis_pct_from_prices(row.get('touch_price'), row.get('fut_touch_price'))
    if filled != '':
        row['basis_pct'] = filled


def _basis_too_wide(value) -> bool:
    """|базис%| > 1 — фьючерсные last%/max%/против на сайте не показываем."""
    if value in (None, ''):
        return False
    try:
        return abs(float(value)) > BASIS_MAX_ABS_PCT
    except (TypeError, ValueError):
        return False


def _hide_wide_basis_fut_cols(row: dict) -> None:
    if not _basis_too_wide(row.get('basis_pct')):
        return
    row['last_fut_percent'] = '—'
    row['max_fut_bounce'] = '—'
    row['fut_adverse_pct'] = '—'


def _csv_optional(value, digits):
    if value in (None, ''):
        return ''
    try:
        return round(float(value), digits)
    except (TypeError, ValueError):
        return ''


def _csv_price(value):
    try:
        number = float(value)
    except (TypeError, ValueError):
        return ''
    if number <= 0:
        return ''
    return number


def _bounce_price_for_row(density, outcome):
    if outcome == OUTCOME_BOUNCE_FUT:
        return _csv_price(density.get('last_fut_bounce_price'))
    if outcome == OUTCOME_BOUNCE:
        return _csv_price(density.get('last_bounce_price'))
    return ''


def _bounce_move_for_row(density, outcome):
    """Ход именно этого отскока, %. Для снятия/пробоя/разъедания пусто."""
    if outcome == OUTCOME_BOUNCE_FUT:
        move = _density_float(density, 'last_fut_bounce_move', 0)
        if move <= 0:
            move = fut_bounce_from_touch(density)
        if move <= 0:
            return ''
        return round(move, 2)
    if outcome != OUTCOME_BOUNCE:
        return ''
    move = _density_float(density, 'last_bounce_move', 0)
    if move <= 0:
        move = bounce_from_touch(density, episode=True)
    if move <= 0:
        return ''
    return round(move, 2)


def _bounce_delay_for_row(density, outcome):
    if outcome == OUTCOME_BOUNCE:
        return density.get('last_bounce_delay_min', '')
    if outcome == OUTCOME_BOUNCE_FUT:
        return density.get('last_fut_bounce_delay_min', '')
    if outcome in (OUTCOME_PULL, OUTCOME_BREAK):
        delay = density.get('last_bounce_delay_min', '')
        if delay in (None, ''):
            delay = density.get('last_fut_bounce_delay_min', '')
        return delay
    return ''


def _row_from_density(exchange: str, density: dict, outcome: str, eaten_ratio) -> dict:
    attach_price_steps(density, exchange=exchange)
    touch_pct = density.get('last_touch_percent')
    if touch_pct not in (None, ''):
        try:
            touch_pct = round(float(touch_pct), 2)
        except (TypeError, ValueError):
            touch_pct = ''
    else:
        touch_pct = ''
    return {
        'closed_at': datetime.now(JOURNAL_TZ).isoformat(timespec='seconds'),
        'exchange': exchange,
        'side': density.get('side', ''),
        'symbol': density.get('symbol', ''),
        'wall_id': ensure_wall_id(exchange, density),
        'wall_price': _density_float(density, 'price', 0),
        'last_price': (
            (
                _density_float(density, 'last_fut_bounce_price', 0)
                or _density_float(density, 'candle_fut_1m_close', 0)
                or _density_float(density, 'current_price', 0)
            )
            if outcome == OUTCOME_BOUNCE_FUT
            else _density_float(density, 'current_price', 0)
        ),
        'outcome': outcome,
        'life_time_min': int(_life_time_sec(density) / 60.0),
        'min_to_first_touch': _minutes_between(
            density.get('first_seen_ts'), density.get('first_touch_ts'),
        ),
        'min_touch_to_bounce': _bounce_delay_for_row(density, outcome),
        'min_touch_to_close': (
            _minutes_between(
                density.get('last_touch_ts') or density.get('first_touch_ts'),
                time.time(),
            )
            if outcome in (OUTCOME_PULL, OUTCOME_BREAK)
            else ''
        ),
        'min_first_touch_to_close': (
            _minutes_between(density.get('first_touch_ts'), time.time())
            if outcome in (OUTCOME_PULL, OUTCOME_BREAK)
            else ''
        ),
        'min_percent': _min_percent_for_row(density),
        'last_percent': _last_percent_for_row(density),
        'last_fut_percent': _last_fut_percent_for_row(density),
        'max_bounce': (
            round(_density_float(density, 'max_bounce', 0), 2)
            if _has_first_touch(density)
            else 0
        ),
        'max_fut_bounce': (
            round(_density_float(density, 'max_fut_bounce', 0), 2)
            if _has_first_touch(density)
            else 0
        ),
        'spot_adverse_pct': _spot_adverse_for_row(density, outcome),
        'bounce_move': _bounce_move_for_row(density, outcome),
        'touch_price': _csv_price(density.get('last_touch_price')),
        'touch_percent': touch_pct,
        'bounce_price': _bounce_price_for_row(density, outcome),
        'touch_count': _as_int(density, 'touch_count'),
        'bounce_count': (
            _as_int(density, 'fut_bounce_count')
            if outcome == OUTCOME_BOUNCE_FUT
            else _as_int(density, 'bounce_count')
        ),
        'eaten_ratio': '' if eaten_ratio is None else round(float(eaten_ratio), 4),
        'attack_eaten_ratio': _csv_optional(density.get('last_touch_eaten_ratio'), 4),
        'remaining_pct': _csv_optional(density.get('remaining_pct'), 2),
        'eaten_usdt': _csv_optional(density.get('eaten_traded_usdt'), 2),
        'order_size_usdt': round(_density_float(density, 'order_size', 0), 2),
        'peak_order_usdt': round(
            _density_float(density, 'peak_order_size', _density_float(density, 'order_size', 0)),
            2,
        ),
        'qty_size': _density_float(density, 'qty_size', 0),
        'volatility': round(_density_float(density, 'avg_size_five_klines', 0), 4),
        'volume_ratio_avg': round(_density_float(density, 'otnosh_avg_volume_klines', 0), 4),
        'volume_ratio_last': round(_density_float(density, 'otnosh_volume_last_kline', 0), 4),
        'volume_ratio_current': round(_density_float(density, 'otnosh_volume_current_kline', 0), 4),
        'found_percent': _csv_optional(density.get('found_percent'), 2),
        'spot_tick_pct': _csv_optional(density.get('spot_tick_pct'), 6),
        'fut_tick_pct': _csv_optional(density.get('fut_tick_pct'), 6),
        'fut_touch_price': _csv_price(density.get('fut_touch_price')),
        'basis_pct': _basis_pct_from_prices(
            density.get('last_touch_price'), density.get('fut_touch_price'),
        ),
        'fut_adverse_pct': _fut_adverse_for_row(density, outcome),
        'price_beyond_episode': 'да' if _as_bool(density, 'price_beyond_episode') else 'нет',
        'btc_move_12x5m': '',
        'btc_touch_12x5m': '',
        'btc_bounce_nx5m': '',
        'chart_id': '',
    }


async def _write_journal_row(exchange: str, density: dict, outcome: str, eaten_ratio) -> str:
    try:
        await ensure_price_steps(exchange, density.get('symbol'))
    except Exception as error:
        print(f"⚠️ {exchange}: шаг цены не подгружен - {error}")
    row = _row_from_density(exchange, density, outcome, eaten_ratio)
    chart_id = make_chart_id(exchange, row['symbol'], row['side'], row['wall_price'])
    try:
        pack = await save_outcome_chart(exchange, density, chart_id, outcome=outcome) or {}
        if pack.get('ok'):
            row['chart_id'] = chart_id
        if pack.get('btc_move') not in (None, ''):
            row['btc_move_12x5m'] = pack['btc_move']
        if pack.get('btc_touch_12x5m') not in (None, ''):
            row['btc_touch_12x5m'] = pack['btc_touch_12x5m']
            density['episode_btc_touch'] = pack['btc_touch_12x5m']
        if pack.get('btc_bounce_nx5m') not in (None, ''):
            row['btc_bounce_nx5m'] = pack['btc_bounce_nx5m']
    except Exception as error:
        print(f"⚠️ {exchange}: график журнала не записан - {error}")

    CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
    with _lock:
        _ensure_csv_header_locked()
        is_new = not CSV_PATH.exists()
        with CSV_PATH.open('a', encoding='utf-8-sig', newline='') as file:
            writer = csv.DictWriter(file, fieldnames=CSV_FIELDS, extrasaction='ignore')
            if is_new:
                writer.writeheader()
            writer.writerow(row)
        _counts[outcome] = _counts.get(outcome, 0) + 1
        total = sum(_counts.values())

    print(
        f"📒 {exchange}: {outcome.upper()} {density.get('side')} {density.get('symbol')} "
        f"стена={row['wall_price']} цена={row['last_price']} "
        f"min%={row['min_percent']} max%={row['max_bounce']} "
        f"max%Ф={row['max_fut_bounce']} "
        f"отск%={row['bounce_move']} "
        f"разбор={row['eaten_ratio']} "
        f"подходов={row['touch_count']} отскоков={row['bounce_count']}"
    )
    if total % 10 == 0:
        print(f"📒 Итого: {_format_counts(_counts)}")
    return outcome


async def record_bounce_outcome(exchange: str, density: dict) -> str | None:
    """Строка журнала в момент отскока. 5 минут жизни не ждут, строки не удаляют."""
    if not has_tracked_snapshot(density):
        return None
    min_pct = _density_float(density, 'min_percent', _density_float(density, 'percent', 100))
    if min_pct >= BOUNCE_MIN_PERCENT:
        return None
    bounce_count = _as_int(density, 'bounce_count')
    written = _as_int(density, 'bounce_rows_written')
    if bounce_count <= written:
        return None
    moves = density.get('bounce_moves')
    if not isinstance(moves, list):
        moves = []
    prices = density.get('bounce_prices')
    if not isinstance(prices, list):
        prices = []
    delays = density.get('bounce_delays')
    if not isinstance(delays, list):
        delays = []
    last_written = None
    while written < bounce_count:
        if written < len(moves):
            density['last_bounce_move'] = moves[written]
        if written < len(prices) and prices[written]:
            density['last_bounce_price'] = prices[written]
        if written < len(delays):
            density['last_bounce_delay_min'] = delays[written]
        need = spot_bounce_threshold(density) if written == 0 else BOUNCE_REVERSAL_PCT
        if _density_float(density, 'last_bounce_move', 0) < need:
            written += 1
            density['bounce_rows_written'] = written
            continue
        last_written = await _write_journal_row(
            exchange, density, OUTCOME_BOUNCE, _stored_eaten_ratio(density),
        )
        written += 1
        density['bounce_rows_written'] = written
    return last_written


async def record_fut_bounce_outcome(exchange: str, density: dict) -> str | None:
    """Строка журнала, когда фьючерс ушёл на 0.8% от точки отсчёта."""
    if not has_tracked_snapshot(density):
        return None
    min_pct = _density_float(density, 'min_percent', _density_float(density, 'percent', 100))
    if min_pct >= BOUNCE_MIN_PERCENT:
        return None
    bounce_count = _as_int(density, 'fut_bounce_count')
    written = _as_int(density, 'fut_bounce_rows_written')
    if bounce_count <= written:
        return None
    moves = density.get('fut_bounce_moves')
    if not isinstance(moves, list):
        moves = []
    prices = density.get('fut_bounce_prices')
    if not isinstance(prices, list):
        prices = []
    delays = density.get('fut_bounce_delays')
    if not isinstance(delays, list):
        delays = []
    last_written = None
    while written < bounce_count:
        if written < len(moves):
            density['last_fut_bounce_move'] = moves[written]
        if written < len(prices) and prices[written]:
            density['last_fut_bounce_price'] = prices[written]
        if written < len(delays):
            density['last_fut_bounce_delay_min'] = delays[written]
        if _density_float(density, 'last_fut_bounce_move', 0) < BOUNCE_REVERSAL_PCT:
            written += 1
            density['fut_bounce_rows_written'] = written
            continue
        last_written = await _write_journal_row(
            exchange, density, OUTCOME_BOUNCE_FUT, _stored_eaten_ratio(density),
        )
        written += 1
        density['fut_bounce_rows_written'] = written
    return last_written


async def record_outcome(exchange: str, density: dict) -> str | None:
    if not has_tracked_snapshot(density):
        return None
    update_peaks(density, closing=True)
    await apply_touch_eaten(exchange, density)
    await record_eaten_attack_outcome(exchange, density)
    await record_touch_outcome(exchange, density)
    await record_bounce_outcome(exchange, density)
    await record_fut_bounce_outcome(exchange, density)
    density.pop('bounce_just_happened', False)
    density.pop('fut_bounce_just_happened', False)
    young = _life_time_sec(density) < MIN_LIFE_TIME
    if (
        young
        and _as_int(density, 'bounce_rows_written') <= 0
        and _as_int(density, 'fut_bounce_rows_written') <= 0
        and _as_int(density, 'eaten_attack_rows_written') <= 0
        and _as_int(density, 'touch_rows_written') <= 0
    ):
        return None

    min_pct = _density_float(density, 'min_percent', _density_float(density, 'percent', 100))
    attacked = _has_first_touch(density)
    if attacked and min_pct >= BOUNCE_MIN_PERCENT:
        return None

    eaten_ratio = await finalize_eaten_on_close(exchange, density)
    if eaten_ratio is None:
        eaten_ratio = _stored_eaten_ratio(density)

    outcome = classify_outcome(density)
    if outcome == OUTCOME_JUNK or outcome == OUTCOME_BOUNCE:
        return None
    if outcome == OUTCOME_PULL and not attacked:
        return None

    return await _write_journal_row(exchange, density, outcome, eaten_ratio)


def _format_counts(counts: dict) -> str:
    return (
        f"отскок={counts.get(OUTCOME_BOUNCE, 0)} "
        f"отскок фьюч={counts.get(OUTCOME_BOUNCE_FUT, 0)} "
        f"подход={counts.get(OUTCOME_TOUCH, 0)} "
        f"снятие={counts.get(OUTCOME_PULL, 0)} "
        f"без атаки={counts.get(OUTCOME_PULL_NO_ATTACK, 0)} "
        f"разъедание={counts.get(OUTCOME_EATEN, 0)} "
        f"пробой={counts.get(OUTCOME_BREAK, 0)}"
    )


def summary() -> dict:
    with _lock:
        return {
            'all': dict(_counts),
            'file': str(CSV_PATH),
            'total': sum(_counts.values()),
        }


def _life_time_min_from_row(row: dict):
    raw = row.get('life_time_min')
    if raw not in (None, ''):
        try:
            return int(float(raw))
        except (TypeError, ValueError):
            pass
    raw = row.get('life_time_sec')
    if raw not in (None, ''):
        try:
            number = float(raw)
        except (TypeError, ValueError):
            return ''
        if number >= 200:
            return int(number / 60.0)
        return int(number)
    return ''


def _situation_key(row: dict) -> str:
    wall_id = str(row.get('wall_id') or '').strip()
    if wall_id:
        return wall_id
    return '|'.join((
        str(row.get('exchange') or ''),
        str(row.get('symbol') or ''),
        str(row.get('side') or ''),
        str(row.get('wall_price') or ''),
    ))


def read_journal(limit: int | None = 40) -> dict:
    counts = {
        OUTCOME_BOUNCE: 0,
        OUTCOME_BOUNCE_FUT: 0,
        OUTCOME_TOUCH: 0,
        OUTCOME_PULL: 0,
        OUTCOME_PULL_NO_ATTACK: 0,
        OUTCOME_BREAK: 0,
        OUTCOME_EATEN: 0,
        OUTCOME_JUNK: 0,
    }
    rows = []
    with _lock:
        if CSV_PATH.exists():
            with CSV_PATH.open('r', encoding='utf-8-sig', newline='') as file:
                rows = list(csv.DictReader(file))
    for row in rows:
        fill_row_price_steps(row)
        fill_row_basis_pct(row)
        _hide_wide_basis_fut_cols(row)
        row['life_time_min'] = _life_time_min_from_row(row)
        outcome = row.get('outcome', '')
        if outcome in counts:
            counts[outcome] += 1
    if limit is None:
        recent = list(reversed(rows))
    elif limit:
        recent = list(reversed(rows[-limit:]))
    else:
        recent = []
    walls = {key for key in (_situation_key(row) for row in rows) if key and key != '|||'}
    return {
        'all': counts,
        'recent': recent,
        'file': str(CSV_PATH),
        'total': len(rows),
        'walls': len(walls),
    }


def _load_counts_from_csv() -> None:
    if not CSV_PATH.exists():
        return
    with _lock:
        _ensure_csv_header_locked()
    journal = read_journal(limit=0)
    _counts.update(journal['all'])


_load_counts_from_csv()
