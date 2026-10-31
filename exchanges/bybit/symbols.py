import aiohttp
import asyncio
import time
from config.params import MIN_VOLUME_24H, MIN_PERCENT_5H, KLINE_LIMIT_SYMBOLS, VOLATILITY_1H_CANDLES, MIN_VOLATILITY_1H_CANDLES, is_ignored_symbol
from utils.density_filter import median_tr_pct_from_klines

class KlineDataFetcher:
    def __init__(self, max_concurrent=20):
        self.max_concurrent = max_concurrent
        self.session = None
    
    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()

    async def get_klines_batch(self, symbols, interval="60", limit=KLINE_LIMIT_SYMBOLS):
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
        """Получение данных одной свечи"""
        url = "https://api.bybit.com/v5/market/kline"
        params = {
            "category": "spot",
            "symbol": symbol,
            "interval": interval,
            "limit": limit
        }
        
        async with self.session.get(url, params=params) as response:
            data = await response.json()
            
            if data.get('retCode') != 0:
                return None
            
            klines = data['result']['list']
            if len(klines) < MIN_VOLATILITY_1H_CANDLES + 2:
                return None
            
            volume_24h = 0
            for kline in klines[1:]:
                volume_24h += float(kline[6])
            
            avg_size_five_klines = median_tr_pct_from_klines(
                klines,
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
    """Оптимизированная функция получения тикеров"""
    base_url = "https://api.bybit.com/v5/market/tickers"
    
    async with aiohttp.ClientSession() as session:
        # Параллельные запросы для spot и futures
        spot_task = session.get(base_url, params={'category': 'spot'})
        futures_task = session.get(base_url, params={'category': 'linear'})
        
        spot_response, futures_response = await asyncio.gather(spot_task, futures_task)
        
        spot_data = await spot_response.json()
        futures_data = await futures_response.json()
        
        if spot_data['retCode'] != 0 or futures_data['retCode'] != 0:
            return []
        
        # Обработка символов
        spot_symbols = {item['symbol'] for item in spot_data['result']['list']}
        futures_symbols = {item['symbol'] for item in futures_data['result']['list']}
        
        common_symbols = spot_symbols & futures_symbols
        list_tickers = [
            symbol for symbol in common_symbols
            if symbol.endswith('USDT') and not is_ignored_symbol(symbol)
        ]
        
        return list_tickers

async def get_symbols():
    """Основная оптимизированная функция"""
    try:
        # Получаем все тикеры
        tickers = await get_tickers_optimized()
        
        # Параллельно получаем данные kline
        async with KlineDataFetcher(max_concurrent=15) as fetcher:
            kline_data = await fetcher.get_klines_batch(tickers)
                
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
        
        return top_symbols
        
    except Exception as e:
        print(f"Ошибка в get_symbols_optimized: {e}")
        return []

# # Тестирование производительности
# async def main():
#     result = await get_symbols_optimized()
#     print(f"Первые 10: {result}")

# if __name__ == "__main__":
#     asyncio.run(main())