"""Контекст BTC для журнала: 12×5m на подходе и ход от подхода до отскока/пробоя."""

import math
import time

BTC_EXCHANGE = 'Binance'
BTC_SYMBOL = 'BTCUSDT'
BTC_MARKET = 'spot'
BTC_CLOSED_BARS = 12
BTC_BAR_SEC = 300.0
BTC_BOUNCE_OUTCOMES = ('отскок', 'отскок фьюч', 'пробой')


def _unix_ts(raw) -> float:
    try:
        ts = float(raw)
    except (TypeError, ValueError):
        return 0.0
    if ts > 1e12:
        ts /= 1000.0
    return ts if ts > 1e9 else 0.0


def _index_open_unix(index) -> list[float]:
    asi8 = getattr(index, 'asi8', None)
    if asi8 is not None:
        return [float(value) / 1e9 for value in asi8]
    opens = []
    for value in index:
        raw = getattr(value, 'value', None)
        if isinstance(raw, (int, float)) and raw > 1e15:
            opens.append(float(raw) / 1e9)
            continue
        if hasattr(value, 'timestamp'):
            try:
                opens.append(float(value.timestamp()))
                continue
            except (TypeError, ValueError, OverflowError):
                pass
        opens.append(_unix_ts(value))
    return opens


def _ohlc_frame(df):
    if df is None or getattr(df, 'empty', True):
        return None
    try:
        return df[['Open', 'Close']].astype(float).sort_index()
    except (KeyError, TypeError, ValueError):
        return None


def _pct_move(open_first, close_last) -> float | None:
    try:
        open_first = float(open_first)
        close_last = float(close_last)
    except (TypeError, ValueError):
        return None
    if open_first <= 0 or close_last <= 0:
        return None
    return round((close_last / open_first - 1.0) * 100.0, 2)


def btc_bars_for_minutes(minutes) -> int:
    """Сколько 5m свечей покрывают интервал: ceil(минуты / 5), минимум 1."""
    try:
        minutes = float(minutes)
    except (TypeError, ValueError):
        minutes = 0.0
    if minutes <= 0:
        return 1
    return max(1, int(math.ceil(minutes / 5.0)))


def _bar_index_at(opens, ts):
    """Индекс 5m свечи, которая была текущей в момент ts. Незакрытая тоже считается."""
    for i, opened in enumerate(opens):
        if opened <= ts < opened + BTC_BAR_SEC:
            return i
    earlier = [i for i, opened in enumerate(opens) if opened <= ts]
    if earlier:
        return earlier[-1]
    later = [i for i, opened in enumerate(opens) if opened >= ts]
    return later[0] if later else None


def btc_move_from_5m_df(df) -> float | None:
    """(close 12-й 5m / open 1-й − 1) × 100. Текущая свеча входит своим close."""
    frame = _ohlc_frame(df)
    if frame is None or len(frame) < BTC_CLOSED_BARS:
        return None
    window = frame.iloc[-BTC_CLOSED_BARS:]
    return _pct_move(window['Open'].iloc[0], window['Close'].iloc[-1])


def btc_move_12_closed_at(df, at_ts) -> float | None:
    """12×5m к моменту at_ts, включая текущую: open 1-й → close 12-й."""
    frame = _ohlc_frame(df)
    at_ts = _unix_ts(at_ts)
    if frame is None or at_ts <= 0:
        return None
    opens = _index_open_unix(frame.index)
    last_i = _bar_index_at(opens, at_ts)
    if last_i is None or last_i + 1 < BTC_CLOSED_BARS:
        return None
    window = frame.iloc[last_i + 1 - BTC_CLOSED_BARS:last_i + 1]
    if len(window) < BTC_CLOSED_BARS:
        return None
    return _pct_move(window['Open'].iloc[0], window['Close'].iloc[-1])


def btc_move_from_touch_to(df, touch_ts, end_ts) -> float | None:
    """От текущей 5m подхода до текущей 5m отскока/пробоя: open первой → close последней."""
    frame = _ohlc_frame(df)
    touch_ts = _unix_ts(touch_ts)
    end_ts = _unix_ts(end_ts)
    if frame is None or touch_ts <= 0:
        return None
    if end_ts < touch_ts:
        end_ts = touch_ts
    n = btc_bars_for_minutes((end_ts - touch_ts) / 60.0)
    opens = _index_open_unix(frame.index)
    start_i = _bar_index_at(opens, touch_ts)
    end_i = _bar_index_at(opens, end_ts)
    if start_i is None:
        return None
    if end_i is None:
        end_i = start_i
    last_i = max(end_i, start_i + n - 1)
    last_i = min(last_i, len(frame) - 1)
    if last_i < start_i:
        last_i = start_i
    window = frame.iloc[start_i:last_i + 1]
    if window.empty:
        return None
    return _pct_move(window['Open'].iloc[0], window['Close'].iloc[-1])


def btc_journal_moves(df, density, outcome, now_ts=None) -> dict:
    """BTC% на запись; подх%/отск% — от текущего подхода, не от первого касания стены."""
    now_ts = time.time() if now_ts is None else _unix_ts(now_ts)
    density = density or {}
    touch_ts = _unix_ts(density.get('last_touch_ts'))
    outcome_text = str(outcome or '')
    result = {
        'btc_move': btc_move_from_5m_df(df),
        'btc_touch_12x5m': None,
        'btc_bounce_nx5m': None,
    }
    stored = density.get('episode_btc_touch')
    if stored not in (None, ''):
        try:
            result['btc_touch_12x5m'] = round(float(stored), 2)
        except (TypeError, ValueError):
            stored = None
    if result['btc_touch_12x5m'] is None and touch_ts > 0:
        result['btc_touch_12x5m'] = btc_move_12_closed_at(df, touch_ts)
    if touch_ts > 0 and outcome_text in BTC_BOUNCE_OUTCOMES:
        end_ts = now_ts
        delay_key = (
            'last_fut_bounce_delay_min'
            if outcome_text == 'отскок фьюч'
            else 'last_bounce_delay_min'
        )
        delay = density.get(delay_key)
        if outcome_text != 'пробой' and delay not in (None, ''):
            try:
                end_ts = touch_ts + float(delay) * 60.0
            except (TypeError, ValueError):
                end_ts = now_ts
        result['btc_bounce_nx5m'] = btc_move_from_touch_to(df, touch_ts, end_ts)
    return result
