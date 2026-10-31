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
        url = "https://www.okx.com/api/v5/market/candles"
        params = {
            "instId": symbol,
            "bar": "5m",
            "limit": KLINE_LIMIT_VOLUME
        }
        
        for attempt in range(self.max_retries + 1):
            try:
                async with self.session.get(url, params=params) as response:
                    if response.status != 200:
                        if attempt == self.max_retries:
                            return 100, 0, 0, 0
                        await asyncio.sleep(1)  # Задержка перед повторной попыткой
                        continue
                        
                    data = await response.json()
                    klines = data['data']
                    
                    # Проверяем структуру ответа
                    if not klines or len(klines) < MIN_MEDIAN_VOLUME_CANDLES + 2:
                        return 100, 0, 0, 0
                    
                    if len(klines[0]) < 8 or len(klines[1]) < 8:
                        return 100, 0, 0, 0
                    
                    # Объём предыдущей и текущей свеч в USDT
                    try:
                        volume_last_kline = float(klines[1][7])
                        volume_current_kline = float(klines[0][7])
                        current_price = float(klines[0][4])
                        candle_ts, candle_high, candle_low = current_candle_extra(klines[0])
                        closed = klines[1:][:MEDIAN_VOLUME_CANDLES]
                        avg_volume_klines = median_usdt_volume(volume[7] for volume in closed)
                        
                        avg_size_five_klines = median_tr_pct_from_klines(
                            klines,
                            newest_first=True,
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
                        return 100, 0, 0, 0
                        
            except asyncio.TimeoutError:
                if attempt == self.max_retries:
                    print(f"Таймаут получения klines для {symbol}")
                    return 100, 0, 0, 0
            except Exception as e:
                if attempt == self.max_retries:
                    print(f"Ошибка получения klines для {symbol}: {e}")
                    return 100, 0, 0, 0
                
            # Задержка перед повторной попыткой
            if attempt < self.max_retries:
                await asyncio.sleep(0.5)
        
        return 100, 0, 0, 0

    async def get_orderbook(self, symbol: str) -> tuple:
        """Асинхронное получение стакана с обработкой ошибок"""
        url = "https://www.okx.com/api/v5/market/books"
        params = {
            "instId": symbol,
            "sz": str(400)
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
                    bids = data['data'][0].get('bids', [])
                    asks = data['data'][0].get('asks', [])
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

    async def get_klines_1m_extremes(self, symbol: str) -> tuple:
        url = "https://www.okx.com/api/v5/market/candles"
        params = {"instId": symbol, "bar": "1m", "limit": str(KLINE_LIMIT_1M)}
        try:
            async with self.session.get(url, params=params) as response:
                if response.status != 200:
                    return 0, 0, 0
                data = await response.json()
                klines = data.get('data') or []
                return extremes_from_klines(klines, newest_first=True)
        except Exception:
            return 0, 0, 0

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
            
            # 5m, затем стакан и 1m
            kline_data = await self.get_klines_info(symbol, target_price)
            percent_range, volume_last_kline, volume_current_kline, avg_volume_klines, avg_size_five_klines, current_price, candle_high, candle_low, candle_ts = unpack_kline_info(kline_data)

            kline_1m = None
            if should_watch_1m(density):
                bids_asks, kline_1m = await asyncio.gather(
                    self.get_orderbook(symbol),
                    self.get_klines_1m_extremes(symbol),
                )
                bids, asks = bids_asks
            else:
                bids, asks = await self.get_orderbook(symbol)

            # Проверяем, что получили валидный стакан
            if not bids and not asks:
                print(f"Пустой стакан для {symbol}")
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
                            if avg_volume_klines <= 0:
                                avg_volume_klines = 0.001
                            
                            # Вычисляем отношения объемов
                            otnosh_volume_last_kline = order_value / volume_last_kline
                            otnosh_volume_current_kline = order_value / volume_current_kline
                            otnosh_avg_volume_klines = order_value / avg_volume_klines

                            density["exchange"] = "OKX"
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
                        else:
                            print(f"Стакан не заполнен для {symbol}")
                except (ValueError, IndexError) as e:
                    print(f"Ошибка парсинга ордера для {symbol}: {e}")
                    continue
            
            # Плотность не найдена или не прошла проверку
            print(f"Плотность не найдена в стакане для {symbol}")
            return None
            
        except Exception as e:
            print(f"Неожиданная ошибка в _process_single_density для {density.get('symbol', 'unknown')}: {e}")
            return density

# Оптимизированная основная функция
async def update_plotnost_time(list_bids: List[Dict], list_asks: List[Dict]) -> List[Dict]:
    """Оптимизированная версия отслеживания времени жизни плотностей"""
    try:
        list_densities = list_bids + list_asks
        
        async with DensityTracker(max_concurrent=5) as tracker:
            results = await tracker.process_density_batch(list_densities)
        
        return results
        
    except Exception as e:
        print(f"Критическая ошибка в update_plotnost_time: {e}")
        return list(list_bids or []) + list(list_asks or [])
    
# asyncio.run(update_plotnost_time([], [{'side': 'ПРОДАЖА', 'symbol': 'PUMP-USDT', 'price': 0.004744, 'order_size': 53330.069752, 'life_time': 61.15154242515564, 'ask_first_seen': 1761965786.7617855, 'qty_size': 11241583.0, 'volume_last_kline': 30816262.0, 'otnosh_volume_last_kline': 0.36479385462130354, 'volume_current_kline': 8039896.0, 'otnosh_volume_current_kline': 1.3982249272876166, 'otnosh_avg_volume_klines': 0.04844602386121979, 'percent': 3.94}, {'side': 'ПРОДАЖА', 'symbol': 'AVNT-USDT', 'price': 0.6902, 'order_size': 62022.09947080001, 'life_time': 61.15157175064087, 'ask_first_seen': 1761965786.7618299, 'qty_size': 89861.054, 'volume_last_kline': 12577.093, 'otnosh_volume_last_kline': 7.144819077031553, 'volume_current_kline': 976.277, 'otnosh_volume_current_kline': 92.04462872729768, 'otnosh_avg_volume_klines': 1.0644984558734256, 'percent': 1.81}]))