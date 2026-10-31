import aiohttp
import mplfinance as mpf
import pandas as pd
import io
import asyncio
from telegram import Bot
from config.api_tg_keys import BOT_TOKEN, GROUP_ID
from telegram.constants import ParseMode

async def get_okx_candles(symbol: str) -> pd.DataFrame:
    """Асинхронное получение свечных данных с OKX"""
    url = "https://www.okx.com/api/v5/market/candles"
    params = {
        "instId": symbol,
        "bar": "5m",
        "limit": "100"
    }
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params) as response:
                data = await response.json()
                
                if data['code'] != "0":
                    print(f"Ошибка API OKX для {symbol}: {data['msg']}")
                    return None
                
                # Создаем DataFrame из данных
                df = pd.DataFrame(data['data'], columns=[
                    "Open time", "Open", "High", "Low", "Close", "Volume",
                    "volCcy", "volCcyQuote", "confirm"
                ])
                
                # Конвертируем timestamp в datetime (уже в мс)
                df["Open time"] = pd.to_datetime(df["Open time"].astype(int), unit="ms")
                df.set_index("Open time", inplace=True)
                
                # Конвертируем числовые колонки в float
                numeric_columns = ["Open", "High", "Low", "Close", "Volume", "volCcy", "volCcyQuote"]
                df[numeric_columns] = df[numeric_columns].astype(float)
                
                # Для графика используем основные колонки
                return df[["Open", "High", "Low", "Close", "Volume"]]
                
    except Exception as e:
        print(f"Ошибка при получении данных для {symbol}: {e}")
        return None

async def send_chart_to_telegram(side, symbol, price, qty, percent, take_profit, stop_loss, volume_last_kline, otnosh_volume_last_kline, volume_current_kline, otnosh_volume_current_kline, otnosh_avg_volume_klines, life_time, avg_size_klines, order_size):
    """Создание и отправка графика в Telegram"""
    # ИСПРАВЛЕННАЯ настройка стиля свечей
    custom_style = mpf.make_mpf_style(
        base_mpf_style="charles",
        marketcolors=mpf.make_marketcolors(
            up="green",    # Зеленые свечи когда цена РАСТЕТ (close > open)
            down="red",    # Красные свечи когда цена ПАДАЕТ (close < open)
            wick={"up": "green", "down": "red"},
            edge={"up": "green", "down": "red"},
            volume="in",   # Цвет объема соответствует цвету свечи
            ohlc="i"       # Используем инвертированные цвета для ohlc
        ),
        gridstyle="--",
        y_on_right=False,
        facecolor="white",  # Белый фон
        edgecolor="black",
        figcolor="white"
    )

    # Получение данных
    df = await get_okx_candles(symbol)
    
    if df is None or df.empty:
        print(f"Не удалось получить данные для {symbol}")
        return

    # Убедимся, что данные отсортированы по времени (от старых к новым)
    if df.index[0] > df.index[-1]:
        df = df.sort_index(ascending=True)
        print("Данные были пересортированы по возрастанию времени")

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
            returnfig=False  # Убедимся, что не возвращаем fig объект
        )
        buffer.seek(0)
    except Exception as e:
        print(f"Ошибка при создании графика: {e}")
        return

    # Форматирование сообщения с копируемым символом
    symbol_formatted = f"<code>{symbol}</code>"
    
    # Форматируем числовые значения для лучшего отображения
    formatted_qty = round(float(qty))
    formatted_order_size = round(float(order_size))
    formatted_life_time = round(float(life_time))
    formatted_avg_size = round(float(avg_size_klines), 2)
    formatted_volume_last = round(float(volume_last_kline))
    formatted_otnosh_last = round(float(otnosh_volume_last_kline), 2)
    formatted_volume_current = round(float(volume_current_kline))
    formatted_otnosh_current = round(float(otnosh_volume_current_kline), 2)
    formatted_otnosh_avg = round(float(otnosh_avg_volume_klines), 2)

    if side.upper() == "ПОКУПКА":
        caption = (
            f"🟢🟢🟢 OKX 🟢🟢🟢\n"
            f"Символ: {symbol_formatted}\n"
            f"Цена: {price}\n"
            f"Кол-во: {formatted_qty}\n"
            f"Объем $: {formatted_order_size}$\n"
            f"Расстояние: {percent}%\n"
            f"Время: {formatted_life_time} сек\n"
            f"--*--*--*--*--*--*--*--*--*--*--*--*--*--*--\n"
            f"🟢Take-profit: {take_profit}%\n"
            f"🔴Stop-loss: {stop_loss}%\n"
            f"--*--*--*--*--*--*--*--*--*--*--*--*--*--*--\n"
            f"Волатильность: {formatted_avg_size}%\n"
            f"Объем_l_5м: {formatted_volume_last}\n"
            f"Отношение l_5м: {formatted_otnosh_last}\n"
            f"Объем_c_5м: {formatted_volume_current}\n"
            f"Отношение c_5м: {formatted_otnosh_current}\n"
            f"--*--*--*--*--*--*--*--*--*--*--*--*--*--*--\n"
            f"Отношение avg_5: {formatted_otnosh_avg}"
        )
    else:
        caption = (
            f"🔴🔴🔴 OKX 🔴🔴🔴\n"
            f"Символ: {symbol_formatted}\n"
            f"Цена: {price}\n"
            f"Кол-во: {formatted_qty}\n"
            f"Объем $: {formatted_order_size}$\n"
            f"Расстояние: {percent}%\n"
            f"Время: {formatted_life_time} сек\n"
            f"--*--*--*--*--*--*--*--*--*--*--*--*--*--*--\n"
            f"🟢Take-profit: {take_profit}%\n"
            f"🔴Stop-loss: {stop_loss}%\n"
            f"--*--*--*--*--*--*--*--*--*--*--*--*--*--*--\n"
            f"Волатильность: {formatted_avg_size}%\n"
            f"Объем_l_5м: {formatted_volume_last}\n"
            f"Отношение l_5м: {formatted_otnosh_last}\n"
            f"Объем_c_5м: {formatted_volume_current}\n"
            f"Отношение c_5м: {formatted_otnosh_current}\n"
            f"--*--*--*--*--*--*--*--*--*--*--*--*--*--*--\n"
            f"Отношение avg_5: {formatted_otnosh_avg}"
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
        print(f"Ошибка при отправке в Telegram: {e}")

async def tg_send_massege_okx(side, symbol, price, qty, percent, take_profit, stop_loss, volume_last_kline, otnosh_volume_last_kline, volume_current_kline, otnosh_volume_current_kline, otnosh_avg_volume_klines, life_time, avg_size_klines, order_size):
    """Основная функция, объединяющая отправку сообщения и графика"""    
    await send_chart_to_telegram(side, symbol, price, qty, percent, take_profit, stop_loss, volume_last_kline, otnosh_volume_last_kline, volume_current_kline, otnosh_volume_current_kline, otnosh_avg_volume_klines, life_time, avg_size_klines, order_size)