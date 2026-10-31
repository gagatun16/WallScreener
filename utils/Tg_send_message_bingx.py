import aiohttp
import mplfinance as mpf
import pandas as pd
import io
import asyncio
from telegram import Bot
from config.api_tg_keys import BOT_TOKEN, GROUP_ID
from telegram.constants import ParseMode
from config.api_keys import APIKEY, SECRETKEY
import time
import hmac
from hashlib import sha256

async def get_binance_candles(symbol: str) -> pd.DataFrame:
    """Асинхронное получение свечных данных с Binance"""
    params = {
        'symbol': symbol,
        'interval': "5m",
        'limit': 100,
        'timestamp': str(int(time.time() * 1000))
    }
    query_string = '&'.join([f"{key}={value}" for key, value in params.items()])
    signature = hmac.new(
        SECRETKEY.encode("utf-8"), 
        query_string.encode("utf-8"), 
        digestmod=sha256
    ).hexdigest()
    url = f"https://open-api.bingx.com/openApi/spot/v2/market/kline?{query_string}&signature={signature}"
    
    headers = {
            'X-BX-APIKEY': APIKEY,
        }
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers) as response:
                data = await response.json()
                
                df = pd.DataFrame(data['data'], columns=[
                    "Open time", "Open", "High", "Low", "Close", "Volume",
                    "Close time", "Quote volume"
                ])
                df["Open time"] = pd.to_datetime(df["Open time"], unit="ms")
                df.set_index("Open time", inplace=True)
                df = df[["Open", "High", "Low", "Close", "Volume"]].astype(float)
                
                # Сортируем по времени от старых к новым
                df = df.sort_index()
                
                return df
    except Exception as e:
        print(f"Ошибка при получении данных для {symbol}: {e}")
        return None

async def send_chart_to_telegram(side, symbol, price, qty, percent, take_profit, stop_loss, volume_last_kline, otnosh_volume_last_kline, volume_current_kline, otnosh_volume_current_kline, otnosh_avg_volume_klines, life_time, avg_size_klines, order_size):
    """Создание и отправка графика в Telegram"""
    # ПРАВИЛЬНАЯ настройка цветов свечей
    custom_style = mpf.make_mpf_style(
        base_mpf_style="charles",
        marketcolors=mpf.make_marketcolors(
            up="green",      # Зеленые свечи - рост (цена закрытия > цены открытия)
            down="red",      # Красные свечи - падение (цена закрытия < цены открытия)
            wick={"up": "green", "down": "red"},
            edge={"up": "green", "down": "red"},
            volume="in"
        ),
        gridstyle="--",
        y_on_right=False
    )

    # Получение данных
    df = await get_binance_candles(symbol)
    
    if df is None or df.empty:
        print(f"Не удалось получить данные для {symbol}")
        return

    # Добавление горизонтальной линии (цены входа)
    price_line = [price] * len(df.index)
    pl = mpf.make_addplot(price_line, color='blue', linestyle='--', width=1)

    # Создание графика в буфере
    buffer = io.BytesIO()
    
    try:
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
            savefig=dict(fname=buffer, format='png', dpi=100, pad_inches=0.25),
            returnfig=False
        )
        buffer.seek(0)
    except Exception as e:
        print(f"Ошибка при создании графика: {e}")
        return

    # Форматирование сообщения с копируемым символом
    symbol_formatted = f"<code>{symbol}</code>"
    if side == "ПОКУПКА":
        caption = (
            f"🟢🟢🟢 BingX 🟢🟢🟢\n"
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
            f"Отношение avg_5: {round(float(otnosh_avg_volume_klines), 2)}"
        )
    else:
        caption = (
            f"🔴🔴🔴 BingX 🔴🔴🔴\n"
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
            f"Отношение avg_5: {round(float(otnosh_avg_volume_klines), 2)}"
        )

    # Отправка графика
    try:
        bot = Bot(token=BOT_TOKEN)
        await bot.send_photo(
            chat_id=GROUP_ID,
            photo=buffer,
            caption=caption,
            parse_mode=ParseMode.HTML
        )
        print(f"График для {symbol} успешно отправлен")
    except Exception as e:
        print(f"Ошибка при отправке графика в Telegram: {e}")

async def tg_send_massege_bingx(side, symbol, price, qty, percent, take_profit, stop_loss, volume_last_kline, otnosh_volume_last_kline, volume_current_kline, otnosh_volume_current_kline, otnosh_avg_volume_klines, life_time, avg_size_klines, order_size):
    """Основная функция, объединяющая отправку сообщения и графика"""    
    await send_chart_to_telegram(side, symbol, price, qty, percent, take_profit, stop_loss, volume_last_kline, otnosh_volume_last_kline, volume_current_kline, otnosh_volume_current_kline, otnosh_avg_volume_klines, life_time, avg_size_klines, order_size)

# Пример использования:
# asyncio.run(tg_send_massege("ПРОДАЖА", "BTC-USDT", 121000, 113.3334, 1.4, 87.45433, 1.45, 0.2, 2000, 1.434344, 500, 5.234343, 480, 1, 1))