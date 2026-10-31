import aiohttp
import mplfinance as mpf
import pandas as pd
import io
import asyncio
from telegram import Bot
from config.api_tg_keys import BOT_TOKEN, GROUP_ID
from telegram.constants import ParseMode

async def get_bitget_candles(symbol: str) -> pd.DataFrame:
    """Асинхронное получение свечных данных с Bybit"""
    url = "https://api.bitget.com/api/v2/spot/market/candles"
    params = {
        "symbol": symbol,
        "granularity": "5min",
        "limit": str(100)
    }
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params) as response:
                data = await response.json()
                
                if data.get('code') != "00000":
                    print(f"Ошибка API Bitget для {symbol}")
                    return None
                
                klines = data['data']
                
                # Создаем DataFrame согласно документации Bybit
                df = pd.DataFrame(klines, columns=[
                    "startTime", "openPrice", "highPrice", "lowPrice", "closePrice", 
                    "volume", "turnover", "Trading"
                ])
                
                # Конвертируем timestamp в datetime
                df["startTime"] = pd.to_datetime(df["startTime"].astype('int64'), unit='ms')
                df.set_index("startTime", inplace=True)
                
                # Конвертируем цены и объемы в float
                df = df[["openPrice", "highPrice", "lowPrice", "closePrice", "volume"]].astype(float)
                
                # Переименовываем колонки для совместимости с mplfinance
                df.columns = ["Open", "High", "Low", "Close", "Volume"]
                
                # Сортируем по времени (от старых к новым)
                df.sort_index(inplace=True)
                
                return df
                
    except Exception as e:
        print(f"Ошибка при получении данных для {symbol}: {e}")
        return None

async def send_chart_to_telegram(side, symbol, price, qty, percent, take_profit, stop_loss, 
                               volume_last_kline, otnosh_volume_last_kline, volume_current_kline, 
                               otnosh_volume_current_kline, otnosh_avg_volume_5_klines, life_time, avg_size_klines, order_size):
    """Создание и отправка графика в Telegram"""
    
    # Получение данных
    df = await get_bitget_candles(symbol)
    
    if df is None or df.empty:
        print(f"Не удалось получить данные для {symbol}")
        return

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

    # Добавление горизонтальной линии (цены входа)
    price_line = [float(price)] * len(df.index)
    pl = mpf.make_addplot(price_line, color='blue', linestyle='--', width=1, label=f'Цена: {price}')

    # Добавление линий для take-profit и stop-loss
    if side == "ПОКУПКА":
        # Для покупки: TP выше цены, SL ниже
        tp_price = float(price) * (1 + float(take_profit) / 100)
        sl_price = float(price) * (1 - float(stop_loss) / 100)
    else:
        # Для продажи: TP ниже цены, SL выше
        tp_price = float(price) * (1 - float(take_profit) / 100)
        sl_price = float(price) * (1 + float(stop_loss) / 100)
    
    tp_line = [tp_price] * len(df.index)
    sl_line = [sl_price] * len(df.index)
    
    tp_plot = mpf.make_addplot(tp_line, color='green', linestyle='-', width=1, label=f'TP: {tp_price:.4f}')
    sl_plot = mpf.make_addplot(sl_line, color='red', linestyle='-', width=1, label=f'SL: {sl_price:.4f}')

    # Создание графика в буфере
    buffer = io.BytesIO()
    
    try:
        mpf.plot(
            df,
            type="candle",
            style=custom_style,
            title=f"{symbol} (5m) - Bitget",
            ylabel="Цена (USDT)",
            volume=True,
            figratio=(12, 8),
            figsize=(12, 8),
            show_nontrading=False,
            addplot=[pl, tp_plot, sl_plot],
            savefig=dict(fname=buffer, format='png', dpi=100, pad_inches=0.25, bbox_inches='tight')
        )
        buffer.seek(0)
    except Exception as e:
        print(f"Ошибка при создании графика для {symbol}: {e}")
        return

    # Форматирование сообщения с копируемым символом
    symbol_formatted = f"<code>{symbol}</code>"
    
    # Безопасное преобразование чисел
    try:
        qty_rounded = round(float(qty))
        order_size_rounded = round(float(order_size))
        life_time_rounded = round(float(life_time))
        avg_size_rounded = round(float(avg_size_klines), 2)
        volume_last_rounded = round(float(volume_last_kline))
        otnosh_last_rounded = round(float(otnosh_volume_last_kline), 2)
        volume_current_rounded = round(float(volume_current_kline))
        otnosh_current_rounded = round(float(otnosh_volume_current_kline), 2)
    except (ValueError, TypeError) as e:
        print(f"Ошибка преобразования чисел для {symbol}: {e}")
        return

    if side == "ПОКУПКА":
        caption = (
            f"🟢🟢🟢 BITGET 🟢🟢🟢\n"
            f"Символ: {symbol_formatted}\n"
            f"Цена: {price}\n"
            f"Кол-во: {qty_rounded}\n"
            f"Объем $: {order_size_rounded}$\n"
            f"Расстояние: {percent}%\n"
            f"Время: {life_time_rounded} сек\n"
            f"--*--*--*--*--*--*--*--*--*--*--*--*--*--*--\n"
            f"🟢Take-profit: {take_profit}%\n"
            f"🔴Stop-loss: {stop_loss}%\n"
            f"--*--*--*--*--*--*--*--*--*--*--*--*--*--*--\n"
            f"Волатильность: {avg_size_rounded}%\n"
            f"Объем_l_5м: {volume_last_rounded}\n"
            f"Отношение l_5м: {otnosh_last_rounded}\n"
            f"Объем_c_5м: {volume_current_rounded}\n"
            f"Отношение c_5м: {otnosh_current_rounded}\n"
            f"--*--*--*--*--*--*--*--*--*--*--*--*--*--*--\n"
            f"Отношение avg_5: {round(float(otnosh_avg_volume_5_klines), 2)}"
        )
    else:
        caption = (
            f"🔴🔴🔴 BITGET 🔴🔴🔴\n"
            f"Символ: {symbol_formatted}\n"
            f"Цена: {price}\n"
            f"Кол-во: {qty_rounded}\n"
            f"Объем $: {order_size_rounded}$\n"
            f"Расстояние: {percent}%\n"
            f"Время: {life_time_rounded} сек\n"
            f"--*--*--*--*--*--*--*--*--*--*--*--*--*--*--\n"
            f"🟢Take-profit: {take_profit}%\n"
            f"🔴Stop-loss: {stop_loss}%\n"
            f"--*--*--*--*--*--*--*--*--*--*--*--*--*--*--\n"
            f"Волатильность: {avg_size_rounded}%\n"
            f"Объем_l_5м: {volume_last_rounded}\n"
            f"Отношение l_5м: {otnosh_last_rounded}\n"
            f"Объем_c_5м: {volume_current_rounded}\n"
            f"Отношение c_5м: {otnosh_current_rounded}\n"
            f"--*--*--*--*--*--*--*--*--*--*--*--*--*--*--\n"
            f"Отношение avg_5: {round(float(otnosh_avg_volume_5_klines), 2)}"
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
        print(f"Ошибка при отправке графика для {symbol}: {e}")

async def tg_send_massege_bitget(side, symbol, price, qty, percent, take_profit, stop_loss, 
                         volume_last_kline, otnosh_volume_last_kline, volume_current_kline, 
                         otnosh_volume_current_kline, otnosh_avg_volume_klines, life_time, avg_size_klines, order_size):
    """Основная функция, объединяющая отправку сообщения и графика"""    
    await send_chart_to_telegram(side, symbol, price, qty, percent, take_profit, stop_loss, 
                               volume_last_kline, otnosh_volume_last_kline, volume_current_kline, 
                               otnosh_volume_current_kline, otnosh_avg_volume_klines, life_time, avg_size_klines, order_size)

# Пример использования
# asyncio.run(tg_send_massege("ПРОДАЖА", "BTCUSDT", 50000, 113.3334, 1.4, 2.0, 1.5, 2000, 1.43, 500, 5.23, 60, 1.5, 48000))