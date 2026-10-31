''' Вычисление Тейка, Стопа '''
import asyncio
#                       0.64         0.22
from config.params import MIN_VOLATILITY, STOP_LOSS

async def get_RM(avg_size_klines):
    if avg_size_klines >= MIN_VOLATILITY:
        take_profit = avg_size_klines
        print(round(take_profit, 4), STOP_LOSS)
        return round(take_profit, 4), STOP_LOSS
    else:
        take_profit = avg_size_klines
        print(round(take_profit, 4), STOP_LOSS)
        return round(take_profit, 4), STOP_LOSS




    # if avg_size_klines <= MIN_VOLATILITY:
    #     take_profit = MIN_TAKE_PROFIT
    #     stop_loss = STOP_LOSS
    #     print(round(take_profit, 4), round(stop_loss, 4))
    #     return round(take_profit, 4), round(stop_loss, 4)
    # else:
    #     # Волатильность от 0.72 до 1.22
    #     if avg_size_klines < MEDIUM_VOLATILITY:
    #         take_profit = avg_size_klines - 0.2
    #         stop_loss = STOP_LOSS
    #         print(round(take_profit, 4), round(stop_loss, 4))
    #         return round(take_profit, 4), round(stop_loss, 4)
    #     else:
    #         # Волатильность от 1.22 до ...
    #         take_profit = HIGH_TAKE_PROFIT
    #         stop_loss = STOP_LOSS
    #         print(round(take_profit, 4), round(stop_loss, 4))
    #         return round(take_profit, 4), round(stop_loss, 4)

# asyncio.run(get_RM(0.8))
# take_profit = avg_size_klines
# stop_loss = ((avg_size_klines - COMMISSION) / 3) - COMMISSION
# percent_order_size = MIN_VOLATILITY / (avg_size_klines / 100)
