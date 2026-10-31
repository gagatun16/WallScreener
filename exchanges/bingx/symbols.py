from config.api_keys import APIKEY, SECRETKEY
import time
import hmac
from hashlib import sha256
import aiohttp
import asyncio
from config.params import MIN_VOLUME_24H, MIN_PERCENT_5H, KLINE_LIMIT_SYMBOLS, VOLATILITY_1H_CANDLES, MIN_VOLATILITY_1H_CANDLES, is_ignored_symbol
from utils.density_filter import median_tr_pct_from_klines
import json

class KlineDataFetcher:
    def __init__(self, max_concurrent=10):
        self.max_concurrent = max_concurrent
        self.session = None
    
    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()

    async def get_klines_batch(self, symbols, interval="1h", limit=KLINE_LIMIT_SYMBOLS):
        """Параллельное получение kline данных для всех символов"""
        semaphore = asyncio.Semaphore(self.max_concurrent)
        
        async def fetch_single(symbol):
            async with semaphore:
                try:
                    return await self._get_kline_data(symbol, interval, limit)
                except Exception as e:
                    return None
        
        tasks = [fetch_single(symbol) for symbol in symbols]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Фильтруем успешные результаты
        valid_results = [r for r in results if r is not None and not isinstance(r, Exception)]
        return valid_results

    async def _get_kline_data(self, symbol, interval, limit):
        """Получение данных одной свечи"""
        params = {
            'symbol': symbol,
            'interval': interval,
            'limit': limit,
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

        async with self.session.get(url, headers=headers) as response:
            if response.status != 200:
                return None
                
            klines = await response.json()
            # print(klines)
            
            if not klines or len(klines) < 2:
                return None
            
            candles = klines['data']
            if not candles or len(candles) < MIN_VOLATILITY_1H_CANDLES + 2:
                return None
            
            volume_24h = 0
            for kline in candles[1:]:
                volume_24h += float(kline[7])
            
            avg_size_five_klines = median_tr_pct_from_klines(
                candles,
                newest_first=True,
                bar_count=VOLATILITY_1H_CANDLES,
                min_bars=MIN_VOLATILITY_1H_CANDLES,
            )
        
            return {
                'symbol': symbol,
                'volume_24h': volume_24h,
                'avg_percent_5h': avg_size_five_klines
            }

async def get_tickers_optimized():
    """Оптимизированная функция получения тикеров с Binance"""
    timestamp = str(int(time.time() * 1000))
    
    # Генерация подписи для spot
    query_string_spot = f"timestamp={timestamp}"
    signature_spot = hmac.new(
        SECRETKEY.encode("utf-8"), 
        query_string_spot.encode("utf-8"), 
        digestmod=sha256
    ).hexdigest()
    
    # Генерация подписи для futures
    query_string_futures = f"timestamp={timestamp}"
    signature_futures = hmac.new(
        SECRETKEY.encode("utf-8"), 
        query_string_futures.encode("utf-8"), 
        digestmod=sha256
    ).hexdigest()
    
    spot_url = f"https://open-api.bingx.com/openApi/spot/v1/common/symbols?{query_string_spot}&signature={signature_spot}"
    futures_url = f"https://open-api.bingx.com/openApi/swap/v2/quote/contracts?{query_string_futures}&signature={signature_futures}"
    
    headers = {
        'X-BX-APIKEY': APIKEY,
    }
    
    async with aiohttp.ClientSession() as session:
        try:
            # Параллельные запросы для spot и futures
            spot_task = session.get(spot_url, headers=headers)
            futures_task = session.get(futures_url, headers=headers)
            
            spot_response, futures_response = await asyncio.gather(spot_task, futures_task)
            
            if spot_response.status != 200 or futures_response.status != 200:
                print(spot_response.status, futures_response.status)
                return []
            
            spot_text = await spot_response.text()
            futures_text = await futures_response.text()

            try:
                spot_data = json.loads(spot_text)
                futures_data = json.loads(futures_text)
            except json.JSONDecodeError as e:
                print(f"JSON decode error: {e}")
                print(f"Spot text: {spot_text}")
                print(f"Futures text: {futures_text}")
                return []
            
            # Обработка спотовых символов
            spot_symbols = set()
            for symbol_info in spot_data['data']['symbols']:
                if symbol_info['symbol'].endswith("-USDT"):
                    spot_symbols.add(symbol_info['symbol'])

            # Обработка фьючерсных символов
            futures_symbols = set()
            for symbol_info in futures_data['data']:
                futures_symbols.add(symbol_info['symbol'])

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
    """Основная оптимизированная функция для Binance"""
    try:
        # Получаем все тикеры
        print("Получаем список символов...")
        tickers = await get_tickers_optimized()
        print(f"Найдено общих символов: {len(tickers)}")
        
        if not tickers:
            return []
        
        # Параллельно получаем данные kline
        print("Анализируем объем и волатильность...")
        async with KlineDataFetcher(max_concurrent=15) as fetcher:
            kline_data = await fetcher.get_klines_batch(tickers)
        
        print(f"Успешно получено данных: {len(kline_data)}")
        
        # Фильтрация результатов
        filtered = []
        for data in kline_data:
            if (data['volume_24h'] > MIN_VOLUME_24H and 
                abs(data['avg_percent_5h']) > MIN_PERCENT_5H):
                filtered.append(data)
        
        # Сортировка по абсолютному изменению цены
        filtered.sort(key=lambda x: abs(x['avg_percent_5h']), reverse=True)
        
        # Ограничение до 50 символов
        top_symbols = [item['symbol'] for item in filtered[:50]]
        
        print(f"Отобрано символов: {len(top_symbols)}")
        return top_symbols
        
    except Exception as e:
        print(f"Ошибка в get_symbols: {e}")
        return []

# asyncio.run(get_symbols())