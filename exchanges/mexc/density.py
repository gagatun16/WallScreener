from config.params import KLINE_LIMIT_VOLUME, KLINE_LIMIT_1M, MEDIAN_VOLUME_CANDLES, MIN_MEDIAN_VOLUME_CANDLES
from utils.density_filter import (
    apply_candle_extremes,
    attach_1m_extremes,
    attach_fut_1m_extremes,
    current_candle_extra,
    extremes_from_klines,
    is_density_wall,
    median_tr_pct_from_klines,
    median_usdt_volume,
    should_watch_1m,
    unpack_kline_info,
)
import asyncio
import time
import aiohttp
from typing import List, Dict, Optional
from utils.check_orderbook import check_orderbook

class DensityTracker:
    def __init__(self, max_concurrent=5):
        self.max_concurrent = max_concurrent
        self.session = None
        self.request_timeout = 10  # Увеличиваем таймаут
        self.max_retries = 2  # Добавляем повторные попытки
    
    async def __aenter__(self):
        # Настраиваем более надежную сессию
        timeout = aiohttp.ClientTimeout(total=self.request_timeout)
        connector = aiohttp.TCPConnector(limit=self.max_concurrent, limit_per_host=10)
        self.session = aiohttp.ClientSession(timeout=timeout, connector=connector)
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()

    async def get_klines_info(self, symbol: str, price_plotnost: float = None) -> tuple:
        """Асинхронное получение данных kline с обработкой ошибок"""
        url = "https://api.mexc.com/api/v3/klines"
        params = {
            "symbol": symbol,
            "interval": "5m", 
            "limit": KLINE_LIMIT_VOLUME
        }
        
        for attempt in range(self.max_retries + 1):
            try:
                async with self.session.get(url, params=params) as response:
                    if response.status != 200:
                        if attempt == self.max_retries:
                            return 100, 0, 0
                        await asyncio.sleep(1)  # Задержка перед повторной попыткой
                        continue
                        
                    klines = await response.json()
                    
                    # Проверяем структуру ответа
                    if not klines or len(klines) < MIN_MEDIAN_VOLUME_CANDLES + 2:
                        return 100, 0, 0
                    
                    if len(klines[-2]) < 8 or len(klines[-1]) < 8:
                        return 100, 0, 0
                    
                    # Объём предыдущей и текущей свеч в USDT
                    try:
                        volume_last_kline = float(klines[-2][7])
                        volume_current_kline = float(klines[-1][7])
                        current_price = float(klines[-1][4])
                        candle_ts, candle_high, candle_low = current_candle_extra(klines[-1])
                        closed = klines[:-1][-MEDIAN_VOLUME_CANDLES:]
                        avg_volume_klines = median_usdt_volume(volume[7] for volume in closed)
                        
                        avg_size_five_klines = median_tr_pct_from_klines(
                            klines,
                            newest_first=False,
                            bar_count=MEDIAN_VOLUME_CANDLES,
                            min_bars=MIN_MEDIAN_VOLUME_CANDLES,
                        )

                        # Расчет процента
                        if price_plotnost and current_price > 0:
                            percent_range = abs((price_plotnost * 100) / current_price - 100)
                        else:
                            percent_range = 100
                            
                        return percent_range, volume_last_kline, volume_current_kline, avg_volume_klines, avg_size_five_klines, current_price, candle_high, candle_low, candle_ts
                    
                    except (ValueError, IndexError, TypeError) as e:
                        print(f"Ошибка парсинга klines для {symbol}: {e}")
                        return 100, 0, 0
                        
            except asyncio.TimeoutError:
                if attempt == self.max_retries:
                    print(f"Таймаут получения klines для {symbol}")
                    return 100, 0, 0
            except Exception as e:
                if attempt == self.max_retries:
                    print(f"Ошибка получения klines для {symbol}: {e}")
                    return 100, 0, 0
                
            # Задержка перед повторной попыткой
            if attempt < self.max_retries:
                await asyncio.sleep(0.5)
        
        return 100, 0, 0

    async def get_klines_1m_extremes(self, symbol: str) -> tuple:
        url = "https://api.mexc.com/api/v3/klines"
        params = {"symbol": symbol, "interval": "1m", "limit": KLINE_LIMIT_1M}
        try:
            async with self.session.get(url, params=params) as response:
                if response.status != 200:
                    return 0, 0, 0
                klines = await response.json()
                return extremes_from_klines(klines, newest_first=False)
        except Exception:
            return 0, 0, 0

    async def get_orderbook(self, symbol: str) -> tuple:
        """Асинхронное получение стакана с обработкой ошибок"""
        url = "https://api.mexc.com/api/v3/depth"
        params = {
            "symbol": symbol,
            "limit": 200
        }
        
        for attempt in range(self.max_retries + 1):
            try:
                async with self.session.get(url, params=params) as response:
                    if response.status != 200:
                        if attempt == self.max_retries:
                            return [], []
                        await asyncio.sleep(1)
                        continue
                        
                    data = await response.json()
                    bids = data.get('bids', [])
                    asks = data.get('asks', [])
                    return bids, asks
                    
            except asyncio.TimeoutError:
                if attempt == self.max_retries:
                    print(f"Таймаут получения стакана для {symbol}")
                    return [], []
            except Exception as e:
                if attempt == self.max_retries:
                    print(f"Ошибка получения стакана для {symbol}: {e}")
                    return [], []
                
            if attempt < self.max_retries:
                await asyncio.sleep(0.5)
        
        return [], []

    async def process_density_batch(self, densities: List[Dict]) -> List[Dict]:
        """Параллельная обработка группы плотностей с улучшенной обработкой ошибок"""
        semaphore = asyncio.Semaphore(self.max_concurrent)
        
        async def process_single(density: Dict):
            async with semaphore:
                try:
                    result = await self._process_single_density(density)
                    if result is None:
                        # Логируем только если это не из-за фильтра процента
                        symbol = density.get('symbol', 'unknown')
                    return result
                except Exception as e:
                    symbol = density.get('symbol', 'unknown')
                    print(f"Критическая ошибка обработки {symbol}: {e}")
                    return density
        
        # Создаем задачи с ограничением
        tasks = []
        for density in densities:
            task = asyncio.create_task(process_single(density))
            tasks.append(task)
        
        # Ждем завершения всех задач
        results = []
        for task in asyncio.as_completed(tasks):
            try:
                result = await task
                if result is not None:
                    results.append(result)
            except Exception as e:
                print(f"Ошибка в задаче: {e}")
                continue
        
        return results

    async def _process_single_density(self, density: Dict) -> Optional[Dict]:
        """Обработка одной плотности с улучшенной логикой"""
        try:
            current_time = time.time()
            symbol = density["symbol"]
            target_price = density["price"]
            side = density['side']
            
            # Параллельно: стакан, 5m и 1m
            orderbook_task = asyncio.create_task(self.get_orderbook(symbol))
            kline_task = asyncio.create_task(self.get_klines_info(symbol, target_price))
            kline_1m = None
            if should_watch_1m(density):
                kline_1m_task = asyncio.create_task(self.get_klines_1m_extremes(symbol))
                bids_asks, kline_data, kline_1m = await asyncio.gather(
                    orderbook_task, kline_task, kline_1m_task
                )
            else:
                bids_asks, kline_data = await asyncio.gather(orderbook_task, kline_task)
            bids, asks = bids_asks
            percent_range, volume_last_kline, volume_current_kline, avg_volume_klines, avg_size_five_klines, current_price, candle_high, candle_low, candle_ts = unpack_kline_info(kline_data)

            # Проверяем, что получили валидный стакан
            if not bids and not asks:
                return density

            # Определяем ключ для времени первого обнаружения
            first_seen_key = 'bid_first_seen' if side == 'ПОКУПКА' else 'ask_first_seen'
            
            # Проверяем наличие плотности в стакане
            orders_to_check = bids if side == 'ПОКУПКА' else asks
            long = side == 'ПОКУПКА'
            
            for order in orders_to_check:
                if len(order) < 2:
                    continue
                    
                try:
                    order_price = float(order[0])
                    order_size = float(order[1])
                    order_value = order_price * order_size
                    
                    if abs(order_price - target_price) < 0.000001 and is_density_wall(order_price, order_size, avg_volume_klines):
                        # Проверка на заполненность стакана
                        flag_zapolnennosti = check_orderbook(bids, asks, target_price, order_size, percent_range, long)
                        
                        if not flag_zapolnennosti:
                            # Плотность найдена и стакан заполнен - обновляем данные
                            if first_seen_key not in density:
                                density[first_seen_key] = current_time
                            
                            density['life_time'] = current_time - density[first_seen_key]
                            
                            # Защита от деления на ноль
                            if volume_last_kline <= 0:
                                volume_last_kline = 0.001
                            if volume_current_kline <= 0:
                                volume_current_kline = 0.001
                            
                            otnosh_volume_last_kline = order_value / volume_last_kline
                            otnosh_volume_current_kline = order_value / volume_current_kline
                            otnosh_avg_volume_klines = order_value / avg_volume_klines

                            density["exchange"] = "MEXC"
                            density["qty_size"] = order_size
                            density["order_size"] = order_value
                            density["volume_last_kline"] = volume_last_kline
                            density['otnosh_volume_last_kline'] = otnosh_volume_last_kline
                            density['volume_current_kline'] = volume_current_kline
                            density['otnosh_volume_current_kline'] = otnosh_volume_current_kline
                            density['otnosh_avg_volume_klines'] = otnosh_avg_volume_klines
                            density['avg_size_five_klines'] = avg_size_five_klines
                            density["percent"] = round(percent_range, 2)
                            density["current_price"] = current_price
                            apply_candle_extremes(density, candle_high, candle_low, candle_ts)
                            await attach_1m_extremes(
                                density,
                                lambda: self.get_klines_1m_extremes(symbol),
                                kline_1m,
                            )
                            await attach_fut_1m_extremes(
                                density, density.get('exchange'), symbol, self.session,
                            )
                            return density
                except (ValueError, IndexError) as e:
                    print(f"Ошибка парсинга ордера для {symbol}: {e}")
                    continue
            
            # Плотность не найдена или не прошла проверку
            return None
            
        except Exception as e:
            print(f"Неожиданная ошибка в _process_single_density для {density.get('symbol', 'unknown')}: {e}")
            return density

# Оптимизированная основная функция
async def update_plotnost_time(list_bids: List[Dict], list_asks: List[Dict]) -> List[Dict]:
    """Оптимизированная версия отслеживания времени жизни плотностей"""
    try:
        list_densities = list_bids + list_asks
        
        async with DensityTracker(max_concurrent=5) as tracker:  # Уменьшаем concurrent для надежности
            results = await tracker.process_density_batch(list_densities)
        
        print(results)
        return results
        
    except Exception as e:
        print(f"Критическая ошибка в update_plotnost_time: {e}")
        return list(list_bids or []) + list(list_asks or [])