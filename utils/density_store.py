import os
import json
import threading
from pathlib import Path

STORE_PATH = Path('data') / 'active_densities.json'

KEEP_KEYS = (
    'side',
    'symbol',
    'price',
    'life_time',
    'percent',
    'current_price',
    'candle_high',
    'candle_low',
    'candle_open_ts',
    'min_percent',
    'max_bounce',
    'max_bounce_price',
    'max_bounce_armed',
    'max_fut_bounce',
    'max_fut_bounce_armed',
    'last_bounce_move',
    'last_bounce_price',
    'bounce_moves',
    'bounce_prices',
    'bounce_rows_written',
    'touch_rows_written',
    'touch_count',
    'bounce_count',
    'bounce_armed',
    'bounce_ref_price',
    'touch_active',
    'touch_bounced',
    'touch_min_percent',
    'touch_candle_ts',
    'touch_in_zone',
    'survived_attack',
    'last_touch_price',
    'last_touch_percent',
    'watch_1m',
    'candle_1m_high',
    'candle_1m_low',
    'candle_1m_ts',
    'candle_1m_bar_high',
    'candle_1m_bar_low',
    'candle_1m_close',
    'm1_ext_high',
    'm1_ext_low',
    'm1_episode_high',
    'm1_episode_low',
    'candle_fut_1m_high',
    'candle_fut_1m_low',
    'candle_fut_1m_ts',
    'candle_fut_1m_bar_high',
    'candle_fut_1m_bar_low',
    'candle_fut_1m_close',
    'fut_episode_high',
    'fut_episode_low',
    'fut_touch_price',
    'fut_touch_candle_ts',
    'fut_touch_pending',
    'fut_bounce_armed',
    'fut_adverse_pct',
    'fut_adverse_price',
    'last_fut_adverse_pct',
    'last_fut_adverse_price',
    'spot_adverse_pct',
    'spot_adverse_price',
    'last_spot_adverse_pct',
    'last_spot_adverse_price',
    'fut_bounce_count',
    'fut_bounce_rows_written',
    'last_fut_bounce_move',
    'last_fut_bounce_price',
    'fut_bounce_moves',
    'fut_bounce_prices',
    'eaten_ratio',
    'remaining_pct',
    'eaten_traded_usdt',
    'last_eaten_ts',
    'last_touch_traded_usdt',
    'last_touch_eaten_ratio',
    'eaten_attack_count',
    'eaten_attack_rows_written',
    'touch_eaten_pending',
    'price_beyond_wall',
    'price_beyond_episode',
    'last_checked_ts',
    'max_order_size',
    'peak_order_size',
    'found_percent',
    'journal_last_percent',
    'journal_last_fut_percent',
    'episode_btc_touch',
    'spot_tick_pct',
    'fut_tick_pct',
    'wall_id',
    'first_touch_ts',
    'last_touch_ts',
    'last_bounce_delay_min',
    'bounce_delays',
    'last_fut_bounce_delay_min',
    'fut_bounce_delays',
    'first_seen_ts',
    'bid_first_seen',
    'ask_first_seen',
    'qty_size',
    'order_size',
    'volume_last_kline',
    'volume_current_kline',
    'otnosh_volume_last_kline',
    'otnosh_volume_current_kline',
    'otnosh_avg_volume_klines',
    'avg_size_five_klines',
    'exchange',
)

_lock = threading.Lock()


def _jsonable(density: dict) -> dict:
    row = {}
    for key in KEEP_KEYS:
        if key not in density:
            continue
        value = density[key]
        if isinstance(value, (str, bool)) or value is None:
            row[key] = value
            continue
        if isinstance(value, int):
            row[key] = value
            continue
        if isinstance(value, list):
            packed = []
            for item in value:
                if item is None:
                    packed.append(None)
                    continue
                try:
                    packed.append(float(item))
                except (TypeError, ValueError):
                    continue
            row[key] = packed
            continue
        try:
            row[key] = float(value)
        except (TypeError, ValueError):
            row[key] = str(value)
    return row


def _read() -> dict:
    if not STORE_PATH.exists():
        return {}
    try:
        return json.loads(STORE_PATH.read_text(encoding='utf-8'))
    except (OSError, json.JSONDecodeError):
        return {}


def _write(payload: dict) -> None:
    STORE_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = STORE_PATH.with_suffix('.tmp')
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
    os.replace(tmp, STORE_PATH)


def save_exchange(name: str, symbols: list, bids: list, asks: list) -> None:
    pack = {
        'symbols': list(symbols or []),
        'bids': [_jsonable(item) for item in bids or [] if item.get('symbol') and item.get('price') is not None],
        'asks': [_jsonable(item) for item in asks or [] if item.get('symbol') and item.get('price') is not None],
    }
    with _lock:
        payload = _read()
        payload[name] = pack
        _write(payload)


def load_exchange(name: str) -> dict:
    with _lock:
        payload = _read()
    pack = payload.get(name) or {}
    return {
        'symbols': list(pack.get('symbols') or []),
        'bids': list(pack.get('bids') or []),
        'asks': list(pack.get('asks') or []),
    }
