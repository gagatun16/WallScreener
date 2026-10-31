# Плотность: размер стены в USDT > MIN_ORDER_SIZE И >= COEFFICIENT × медиана объёма 5m в USDT
MIN_ORDER_SIZE = 50_000
MIN_VOLUME_24H = 200_000  # USDT

# Объём для 3×: медиана закрытых 5m-свечей
MEDIAN_VOLUME_CANDLES = 24
MIN_MEDIAN_VOLUME_CANDLES = 12
KLINE_LIMIT_VOLUME = 26  # 1 prev + 24 закрытых + текущая
KLINE_LIMIT_1M = 5  # 1m High/Low: подходы, max% и отскок

# Волатильность при отборе монет: медиана TR% по часовикам
VOLATILITY_1H_CANDLES = 12
MIN_VOLATILITY_1H_CANDLES = 8
KLINE_LIMIT_SYMBOLS = 14  # 1 prev + 12 закрытых + текущая

# Time
MIN_PERCENT_5H = 2
MIN_LIFE_TIME = 300  # журнал: жизнь ≥ 5 минут
MIN_LIFE_TIME_SITE = 300  # на сайте только плотности старше 5 минут

# Percent range
BOUNCE_MIN_PERCENT = 0.3  # журнал: писать только если min% < 0.3%; отскок — тот же порог подхода
BOUNCE_REVERSAL_PCT = 0.8  # повторный спот-отскок от цены касания; фьючерс от входа фьюч
SPOT_BOUNCE_REVERSAL_PCT = 1.1  # первый спот-отскок: уход >= 1.1% от стены после касания 0.00%
PULL_MAX_PERCENT = 1.0  # снятие: min% < 1%; дальше — мусор
BASIS_MAX_ABS_PCT = 1.0  # журнал: last% Ф / max% Ф / против Ф = «—», если |базис%| > 1

# Разбор стены по ленте сделок на цене p: eaten_ratio = объём_сделок_по_p / размер_стены
EATEN_BREAK_RATIO = 1.0      # пробой: разбор >= 1.0 от актуального пика И цена ушла за стену
EATEN_PARTIAL_RATIO = 0.2    # [0.2, 1) — разъедание и снятие; < 0.2 — снятие
TRADE_LOOKBACK_SEC = 120     # окно ленты при закрытии стены, сек

MIN_VOLATILITY = 0.64
STOP_LOSS = 0.22
COMMISSION = 0.04
COEFFICIENT_SIZE_OF_PLOTNOST = 3

# Монеты, которые сканер не отбирает и не сопровождает.
IGNORE_SYMBOLS = frozenset({
    'PEPEUSDT',
    'PEPE-USDT',
})


def is_ignored_symbol(symbol) -> bool:
    if not symbol:
        return False
    return str(symbol).strip().upper() in IGNORE_SYMBOLS