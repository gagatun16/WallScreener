import aiohttp
import asyncio
import time
from typing import Dict, List, Tuple, Optional
from utils.density_filter import is_density_wall, median_usdt_volume, collect_density_walls, iter_density_hits
from config.params import KLINE_LIMIT_VOLUME, MEDIAN_VOLUME_CANDLES, MIN_MEDIAN_VOLUME_CANDLES

class OrderBookAnalyzer:
    def __init__(self, max_concurrent=10):
        self.max_concurrent = max_concurrent
        self.session = None
    
    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()

    async def _get_avg_volume(self, symbol: str) -> Optional[float]:
        """Медиана объёма 24 закрытых 5m-свечей в USDT."""
        url = "https://api.bybit.com/v5/market/kline"
        params = {"category": "spot", "symbol": symbol, "interval": "5", "limit": KLINE_LIMIT_VOLUME}
        try:
            async with self.session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=10)) as response:
                data = await response.json()
                if data.get('retCode') != 0:
                    return None
                klines = data.get('result', {}).get('list', [])
                if not klines or len(klines) < MIN_MEDIAN_VOLUME_CANDLES + 1:
                    return None
                closed = klines[1:][:MEDIAN_VOLUME_CANDLES]
                return median_usdt_volume(k[6] for k in closed)
        except Exception:
            return None

    async def process_symbol_batch(self, symbols: List[str]) -> List[Tuple]:
        """Параллельная обработка группы символов"""
        semaphore = asyncio.Semaphore(self.max_concurrent)
        
        async def process_single(symbol: str):
            async with semaphore:
                try:
                    return await self._fetch_order_book(symbol)
                except Exception as e:
                    print(f"Ошибка обработки {symbol}: {e}")
                    return None, None
        
        tasks = [process_single(symbol) for symbol in symbols]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Фильтруем валидные результаты
        valid_results = []
        for result in results:
            if isinstance(result, Exception) or result is None:
                continue
            valid_results.append(result)
        
        return valid_results

    async def _fetch_order_book(self, symbol: str) -> Tuple[Optional[Dict], Optional[Dict]]:
        """Получение и анализ стакана для одного символа"""
        url = "https://api.bybit.com/v5/market/orderbook"
        params = {
            "category": "spot", 
            "symbol": symbol, 
            "limit": 200
        }
        
        async with self.session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=10)) as response:
            data = await response.json()
            
            if data.get('retCode') != 0:
                return None, None
            
            bids = data['result'].get('b', [])
            asks = data['result'].get('a', [])
            
            avg_volume = await self._get_avg_volume(symbol)
            if not avg_volume:
                return None, None
            
            found_bids = collect_density_walls(bids, "ПОКУПКА", symbol, avg_volume)
            found_asks = collect_density_walls(asks, "ПРОДАЖА", symbol, avg_volume)
            return found_bids, found_asks

async def analyze_order_books(array_symbols: List[str]) -> Tuple[List[Dict], List[Dict]]:
    """Оптимизированная основная функция"""
    try:
        async with OrderBookAnalyzer(max_concurrent=10) as analyzer:
            results = await analyzer.process_symbol_batch(array_symbols)
        
        buy_list = []
        sell_list = []
        
        for result_bid, result_ask in results:
            buy_list.extend(iter_density_hits(result_bid))
            sell_list.extend(iter_density_hits(result_ask))
        
        print(f"Найдено плотностей: BUY={len(buy_list)}, SELL={len(sell_list)}")
        print(buy_list, sell_list)
        return buy_list, sell_list
        
    except Exception as e:
        print(f"Ошибка в analyze_order_books_optimized: {e}")
        return [], []