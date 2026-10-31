''' Отбор монет с плотностями '''
import asyncio
import time
import aiohttp
from typing import Dict, List, Tuple, Optional
from utils.density_filter import is_density_wall, median_usdt_volume, collect_density_walls, iter_density_hits
from config.params import KLINE_LIMIT_VOLUME, MEDIAN_VOLUME_CANDLES, MIN_MEDIAN_VOLUME_CANDLES

class OrderBookAnalyzer:
    def __init__(self, max_concurrent=10):
        self.max_concurrent = max_concurrent
        self.session = None
        self.max_retries = 3  # Количество повторных попыток
        self.retry_delay = 0.5  # Задержка между попытками в секундах
    
    async def __aenter__(self):
        # Настраиваем более надежную сессию
        timeout = aiohttp.ClientTimeout(total=10)
        connector = aiohttp.TCPConnector(limit=self.max_concurrent, limit_per_host=10)
        self.session = aiohttp.ClientSession(timeout=timeout, connector=connector)
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()

    async def _get_kline_data(self, symbol, interval, limit):
        """Получение данных одной свечи"""
        url = "https://api.binance.com/api/v3/klines"
        params = {
            "symbol": symbol,
            "interval": interval,
            "limit": limit
        }
        
        try:
            async with self.session.get(url, params=params) as response:
                if response.status != 200:
                    return None
                    
                klines = await response.json()
                
                if not klines or len(klines) < MIN_MEDIAN_VOLUME_CANDLES + 1:
                    return None
                
                closed = klines[:-1][-MEDIAN_VOLUME_CANDLES:]
                avg_volume_5_klines = median_usdt_volume(kline[7] for kline in closed)
                
                return { 'avg_volume_5_klines': avg_volume_5_klines }
        except Exception as e:
            print(f"Ошибка получения данных свечей для {symbol}: {e}")
            return None

    async def get_orderbook_batch(self, symbols: List[str]) -> List[Tuple]:
        semaphore = asyncio.Semaphore(self.max_concurrent)
        
        async def process_single_with_retry(symbol: str):
            """Обработка одного символа с повторными попытками"""
            last_error = None
            
            for attempt in range(self.max_retries + 1):
                try:
                    async with semaphore:
                        return await self._fetch_orderbook(symbol)
                        
                except asyncio.TimeoutError as e:
                    last_error = f"Таймаут для {symbol} (попытка {attempt + 1}/{self.max_retries + 1})"
                    if attempt == self.max_retries:
                        print(last_error)
                except aiohttp.ClientError as e:
                    last_error = f"Сетевая ошибка для {symbol}: {e} (попытка {attempt + 1}/{self.max_retries + 1})"
                    if attempt == self.max_retries:
                        print(last_error)
                except Exception as e:
                    last_error = f"Ошибка обработки {symbol}: {e} (попытка {attempt + 1}/{self.max_retries + 1})"
                    if attempt == self.max_retries:
                        print(last_error)
                
                # Задержка перед повторной попыткой (увеличивается с каждой попыткой)
                if attempt < self.max_retries:
                    await asyncio.sleep(self.retry_delay * (attempt + 1))
            
            return None, None
        
        tasks = [process_single_with_retry(symbol) for symbol in symbols]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Фильтруем валидные результаты
        valid_results = []
        error_count = 0
        
        for result in results:
            if isinstance(result, Exception):
                error_count += 1
                continue
            if result is None:
                continue
            # Проверяем, что это кортеж с двумя элементами
            if isinstance(result, tuple) and len(result) == 2:
                valid_results.append(result)
        
        if error_count > 0:
            print(f"Завершено с ошибками: {error_count} символов не обработано")
        
        return valid_results

    async def _fetch_orderbook(self, symbol: str) -> Tuple[Optional[Dict], Optional[Dict]]:
        """Получение и анализ стакана для одного символа с улучшенной обработкой ошибок"""
        url = "https://api.binance.com/api/v3/depth"
        params = {
            "symbol": symbol,
            "limit": 200
        }
        
        try:
            async with self.session.get(url, params=params) as response:
                if response.status != 200:
                    # Логируем HTTP ошибки для диагностики
                    if response.status == 429:
                        raise Exception(f"Rate limit для {symbol}")
                    elif response.status == 418:
                        raise Exception(f"IP banned для {symbol}")
                    elif response.status >= 500:
                        raise Exception(f"Server error {response.status} для {symbol}")
                    else:
                        raise Exception(f"HTTP {response.status} для {symbol}")
                
                data = await response.json()
                
                # Проверяем структуру ответа
                if 'bids' not in data or 'asks' not in data:
                    raise Exception(f"Некорректный ответ для {symbol}")
                
                bids = data.get('bids', [])
                asks = data.get('asks', [])
                
                # Получаем данные свечей
                kline_data = await self._get_kline_data(symbol, "5m", KLINE_LIMIT_VOLUME)
                if not kline_data:
                    return None, None
                avg_volume_5_klines = kline_data['avg_volume_5_klines']
                
                found_bids = collect_density_walls(bids, "ПОКУПКА", symbol, avg_volume_5_klines)
                found_asks = collect_density_walls(asks, "ПРОДАЖА", symbol, avg_volume_5_klines)
                return found_bids, found_asks
                
        except asyncio.TimeoutError:
            raise
        except aiohttp.ClientError:
            raise
        except Exception as e:
            # Пробрасываем все исключения для обработки в process_single_with_retry
            raise

async def analyze_order_books_optimized(array_symbols: List[str]) -> Tuple[List[Dict], List[Dict]]:
    """Оптимизированная версия анализа стаканов с повторными попытками"""
    try:
        if not array_symbols:
            print("Нет символов для анализа")
            return [], []
        
        print(f"Начало анализа {len(array_symbols)} символов...")
        start_time = time.time()
        
        async with OrderBookAnalyzer(max_concurrent=10) as analyzer:
            results = await analyzer.get_orderbook_batch(array_symbols)
        
        buy_list = []
        sell_list = []
        
        for result_bid, result_ask in results:
            buy_list.extend(iter_density_hits(result_bid))
            sell_list.extend(iter_density_hits(result_ask))
        
        processing_time = time.time() - start_time
        print(f"Анализ завершен за {processing_time:.2f} секунд")
        print(f"Найдено плотностей: BUY={len(buy_list)}, SELL={len(sell_list)}")
        
        return buy_list, sell_list
        
    except Exception as e:
        print(f"Критическая ошибка в analyze_order_books_optimized: {e}")
        return [], []