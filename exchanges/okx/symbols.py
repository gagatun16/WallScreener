import aiohttp
import asyncio
import time
from config.params import MIN_VOLUME_24H, MIN_PERCENT_5H, KLINE_LIMIT_SYMBOLS, VOLATILITY_1H_CANDLES, MIN_VOLATILITY_1H_CANDLES, is_ignored_symbol
from utils.density_filter import median_tr_pct_from_klines

class KlineDataFetcher:
    def __init__(self, max_concurrent=5):
        self.max_concurrent = max_concurrent
        self.session = None
    
    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()

    async def get_klines_batch(self, symbols, interval="1H", limit=KLINE_LIMIT_SYMBOLS):
        """Параллельное получение kline данных для всех символов"""
        semaphore = asyncio.Semaphore(self.max_concurrent)
        
        async def fetch_single(symbol):
            async with semaphore:
                try:
                    return await self._get_kline_data(symbol, interval, limit)
                except Exception as e:
                    print(f"Ошибка получения данных для {symbol}: {e}")
                    return None
        
        tasks = [fetch_single(symbol) for symbol in symbols]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Фильтруем успешные результаты
        valid_results = [r for r in results if r is not None and not isinstance(r, Exception)]
        return valid_results

    async def _get_kline_data(self, symbol, interval, limit):
        """Версия с детальной отладкой"""
        url = "https://www.okx.com/api/v5/market/candles"
        params = {
            "instId": symbol,
            "bar": interval,
            "limit": limit
        }
        
        try:
            async with self.session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=10)) as response:
                if response.status != 200:
                    print(f"❌ HTTP {response.status} for {symbol}")
                    return None
                    
                data = await response.json()
                
                # Проверяем код ответа OKX API
                if data.get('code') != '0':
                    print(f"❌ API error for {symbol}: {data.get('msg')}")
                    return None
                
                klines = data.get('data', [])
                
                # Детальная отладка
                if not klines:
                    print(f"❌ No klines data for {symbol}")
                    return None
                    
                if len(klines) < MIN_VOLATILITY_1H_CANDLES + 2:
                    print(f"❌ Not enough klines for {symbol}: {len(klines)}")
                    return None
                
                volume_24h = 0
                for kline in klines[1:]:
                    try:
                        if not isinstance(kline, list) or len(kline) < 8:
                            continue
                        volume_24h += float(kline[7])
                    except (ValueError, IndexError, TypeError) as e:
                        print(f"⚠️ Kline processing error for {symbol}: {e}")
                        continue
                
                avg_size_five_klines = median_tr_pct_from_klines(
                    klines,
                    newest_first=True,
                    bar_count=VOLATILITY_1H_CANDLES,
                    min_bars=MIN_VOLATILITY_1H_CANDLES,
                )
                if not avg_size_five_klines:
                    print(f"❌ No valid klines processed for {symbol}")
                    return None
                
                result = {
                    'symbol': symbol,
                    'volume_24h': volume_24h,
                    'avg_percent_5h': avg_size_five_klines
                }
                return result
                
        except asyncio.TimeoutError:
            print(f"⏰ Timeout for {symbol}")
            return None
        except Exception as e:
            print(f"❌ Request error for {symbol}: {e}")
            return None

async def get_tickers_optimized():
    """Оптимизированная функция получения тикеров с Binance"""
    spot_url = "https://www.okx.com/api/v5/market/tickers"
    futures_url = "https://www.okx.com/api/v5/market/tickers"
    
    async with aiohttp.ClientSession() as session:
        try:
            # Параллельные запросы для spot и futures
            spot_task = session.get(spot_url, params={"instType": "SPOT"})
            futures_task = session.get(futures_url, params={"instType": "SWAP"})
            
            spot_response, futures_response = await asyncio.gather(spot_task, futures_task)
            
            if spot_response.status != 200 or futures_response.status != 200:
                print(spot_response.status, futures_response.status)
                return []
            
            spot_data = await spot_response.json()
            futures_data = await futures_response.json()
            
            # Обработка спотовых символов
            spot_symbols = set()
            for symbol_info in spot_data['data']:
                if symbol_info['instId'].endswith('-USDT'):
                    spot_symbols.add(symbol_info['instId'])
            # Обработка фьючерсных символов
            futures_symbols = set()
            for symbol_info in futures_data['data']:
                futures_symbols.add(symbol_info['instId'].replace("-SWAP", ""))

            # Общие символы для spot и futures
            common_symbols = spot_symbols & futures_symbols
            
            # Фильтрация плохих символов
            bad_strings = ['UP', 'DOWN', 'BULL', 'BEAR', 'BIDR', 'DAI', 'FDUSD', 'USDP', 'USD1']
            filtered_symbols = [
                symbol for symbol in common_symbols
                if not any(bad_str in symbol for bad_str in bad_strings)
                and not is_ignored_symbol(symbol)
            ]
            
            return filtered_symbols
            
        except Exception as e:
            print(f"Ошибка при получении тикеров: {e}")
            return []

async def get_symbols():
    """Основная функция с улучшенной отладкой"""
    try:
        tickers = await get_tickers_optimized()
        
        if not tickers:
            return []
        
        # Параллельно получаем данные kline
        async with KlineDataFetcher(max_concurrent=5) as fetcher:  # Уменьшим concurrent
            kline_data = await fetcher.get_klines_batch(tickers, interval="1H", limit=KLINE_LIMIT_SYMBOLS)
        
        # Фильтрация результатов
        filtered = []
        for data in kline_data:
            if (data['volume_24h'] > MIN_VOLUME_24H and 
                abs(data['avg_percent_5h']) > MIN_PERCENT_5H):
                filtered.append(data)
        
        print(f"После фильтрации: {len(filtered)} символов")
        
        # Сортировка по абсолютному изменению цены
        filtered.sort(key=lambda x: abs(x['avg_percent_5h']), reverse=True)
        
        # Ограничение до 50 символов
        top_symbols = [item['symbol'] for item in filtered[:50]]
        
        return top_symbols
        
    except Exception as e:
        print(f"Ошибка в get_symbols: {e}")
        import traceback
        traceback.print_exc()
        return []

# asyncio.run(get_symbols())