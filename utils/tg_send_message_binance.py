import aiohttp
import mplfinance as mpf
import pandas as pd
import io
import asyncio
from telegram import Bot
from config.api_tg_keys import BOT_TOKEN, GROUP_ID
from telegram.constants import ParseMode

async def get_binance_candles(symbol: str) -> pd.DataFrame:
    """Асинхронное получение свечных данных с Binance"""
    url = "https://api.binance.com/api/v3/klines"
    params = {"symbol": symbol, "interval": "5m", "limit": 100}
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params) as response:
                data = await response.json()
                
                df = pd.DataFrame(data, columns=[
                    "Open time", "Open", "High", "Low", "Close", "Volume",
                    "Close time", "Quote volume", "Trades", "Taker buy base", "Taker buy quote", "Ignore"
                ])
                df["Open time"] = pd.to_datetime(df["Open time"], unit="ms")
                df.set_index("Open time", inplace=True)
                df = df[["Open", "High", "Low", "Close", "Volume"]].astype(float)
                
                return df
    except Exception as e:
        print(f"Ошибка при получении данных для {symbol}: {e}")
        return None

async def send_chart_to_telegram(side, symbol, price, qty, percent, take_profit, stop_loss, volume_last_kline, otnosh_volume_last_kline, volume_current_kline, otnosh_volume_current_kline, otnosh_avg_volume_5_klines, life_time, avg_size_klines, order_size):
    """Создание и отправка графика в Telegram"""
    # Настройка стиля свечей
    custom_style = mpf.make_mpf_style(
        base_mpf_style="charles",
        marketcolors=mpf.make_marketcolors(
            up="green", down="red",
            wick={"up": "green", "down": "red"},
            edge={"up": "green", "down": "red"},
            volume="in"
        ),
        gridstyle="--",
        y_on_right=False
    )

    # Получение данных
    df = await get_binance_candles(symbol)

    # Добавление горизонтальной линии (цены входа)
    price_line = [price] * len(df.index)
    pl = mpf.make_addplot(price_line, color='blue', linestyle='--', width=1)

    # Создание графика в буфере
    buffer = io.BytesIO()
    mpf.plot(
        df,
        type="candle",
        style=custom_style,
        title=f"{symbol} (5m)",
        ylabel="Цена ($)",
        volume=True,
        figratio=(12, 6),
        show_nontrading=False,
        addplot=[pl],
        savefig=dict(fname=buffer, format='png', dpi=100, pad_inches=0.25)
    )
    buffer.seek(0)

    # Форматирование сообщения с копируемым символом
    symbol_formatted = f"<code>{symbol}</code>"
    if side == "ПОКУПКА":
        caption = (
            f"🟢🟢🟢 BINANCE 🟢🟢🟢\n"
            f"Символ: {symbol_formatted}\n"
            f"Цена: {price}\n"
            f"Кол-во: {round(float(qty))}\n"
            f"Объем $: {round(float(order_size))}$\n"
            f"Расстояние: {percent}%\n"
            f"Время: {round(float(life_time))} сек\n"
            f"--*--*--*--*--*--*--*--*--*--*--*--*--*--*--\n"
            f"🟢Take-profit: {take_profit}%\n"
            f"🔴Stop-loss: {stop_loss}%\n"
            f"--*--*--*--*--*--*--*--*--*--*--*--*--*--*--\n"
            f"Волатильность: {round(avg_size_klines, 2)}%\n"
            f"Объем__5м: {round(float(volume_last_kline))}\n"
            f"Отношение l_5м: {round(float(otnosh_volume_last_kline), 2)}\n"
            f"Объем_c_5м: {round(float(volume_current_kline))}\n"
            f"Отношение c_5м: {round(float(otnosh_volume_current_kline), 2)}\n"
            f"--*--*--*--*--*--*--*--*--*--*--*--*--*--*--\n"
            f"Отношение avg_5: {round(float(otnosh_avg_volume_5_klines), 2)}"
        )
    else:
        caption = (
            f"🔴🔴🔴 BINANCE 🔴🔴🔴\n"
            f"Символ: {symbol_formatted}\n"
            f"Цена: {price}\n"
            f"Кол-во: {round(float(qty))}\n"
            f"Объем $: {round(float(order_size))}$\n"
            f"Расстояние: {percent}%\n"
            f"Время: {round(float(life_time))} сек\n"
            f"--*--*--*--*--*--*--*--*--*--*--*--*--*--*--\n"
            f"🟢Take-profit: {take_profit}%\n"
            f"🔴Stop-loss: {stop_loss}%\n"
            f"--*--*--*--*--*--*--*--*--*--*--*--*--*--*--\n"
            f"Волатильность: {round(avg_size_klines, 2)}%\n"
            f"Объем_l_5м: {round(float(volume_last_kline))}\n"
            f"Отношение l_5м: {round(float(otnosh_volume_last_kline), 2)}\n"
            f"Объем_c_5м: {round(float(volume_current_kline))}\n"
            f"Отношение c_5м: {round(float(otnosh_volume_current_kline), 2)}\n"
            f"--*--*--*--*--*--*--*--*--*--*--*--*--*--*--\n"
            f"Отношение avg_5: {round(float(otnosh_avg_volume_5_klines), 2)}"
        )

    # Отправка графика
    bot = Bot(token=BOT_TOKEN)
    await bot.send_photo(
        chat_id=GROUP_ID,
        photo=buffer,
        caption=caption,
        parse_mode=ParseMode.HTML
    )

async def tg_send_massege_binance(side, symbol, price, qty, percent, take_profit, stop_loss, volume_last_kline, otnosh_volume_last_kline, volume_current_kline, otnosh_volume_current_kline, otnosh_avg_volume_5_klines, life_time, avg_size_klines, order_size):
    """Основная функция, объединяющая отправку сообщения и графика"""    
    await send_chart_to_telegram(side, symbol, price, qty, percent, take_profit, stop_loss, volume_last_kline, otnosh_volume_last_kline, volume_current_kline, otnosh_volume_current_kline, otnosh_avg_volume_5_klines, life_time, avg_size_klines, order_size)

# asyncio.run(send_chart_to_telegram("ПРОДАЖА", "BTCUSDT", 121000, 113.3334, 1.4, 87.45433, 1.45, 0.2, 2000, 1.434344, 500, 5.234343, 480))