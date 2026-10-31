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
    
    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()

    async def _get_avg_volume(self, symbol: str) -> Optional[float]:
        """Медиана объёма 24 закрытых 5m-свечей в USDT."""
        url = "https://api.mexc.com/api/v3/klines"
        params = {"symbol": symbol, "interval": "5m", "limit": KLINE_LIMIT_VOLUME}
        try:
            async with self.session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=5)) as response:
                if response.status != 200:
                    return None
                klines = await response.json()
                if not klines or len(klines) < MIN_MEDIAN_VOLUME_CANDLES + 1:
                    return None
                closed = klines[:-1][-MEDIAN_VOLUME_CANDLES:]
                return median_usdt_volume(k[7] for k in closed)
        except Exception:
            return None

    async def get_orderbook_batch(self, symbols: List[str]) -> List[Tuple]:
        """Параллельное получение стаканов для группы символов"""
        semaphore = asyncio.Semaphore(self.max_concurrent)
        
        async def process_single(symbol: str):
            async with semaphore:
                try:
                    return await self._fetch_orderbook(symbol)
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

    async def _fetch_orderbook(self, symbol: str) -> Tuple[Optional[Dict], Optional[Dict]]:
        """Получение и анализ стакана для одного символа"""
        url = "https://api.mexc.com/api/v3/depth"
        params = {
            "symbol": symbol,
            "limit": 200
        }
        
        async with self.session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=5)) as response:
            if response.status != 200:
                return None, None
                
            data = await response.json()
            
            bids = data.get('bids', [])
            asks = data.get('asks', [])
            
            avg_volume = await self._get_avg_volume(symbol)
            if not avg_volume:
                return None, None
            
            found_bids = collect_density_walls(bids, "ПОКУПКА", symbol, avg_volume)
            found_asks = collect_density_walls(asks, "ПРОДАЖА", symbol, avg_volume)
            return found_bids, found_asks

async def analyze_order_books_optimized(array_symbols: List[str]) -> Tuple[List[Dict], List[Dict]]:
    """Оптимизированная версия анализа стаканов"""
    try:
        async with OrderBookAnalyzer(max_concurrent=10) as analyzer:
            results = await analyzer.get_orderbook_batch(array_symbols)
        
        buy_list = []
        sell_list = []
        
        for result_bid, result_ask in results:
            buy_list.extend(iter_density_hits(result_bid))
            sell_list.extend(iter_density_hits(result_ask))
        
        print(f"Найдено плотностей: BUY={len(buy_list)}, SELL={len(sell_list)}")
        return buy_list, sell_list
        
    except Exception as e:
        print(f"Ошибка в analyze_order_books_optimized: {e}")
        return [], []
# [{'side': 'ПОКУПКА', 'symbol': 'MEUSDT', 'price': 0.482, 'life_time': 1761713288.8855112}, {'side': 'ПОКУПКА', 'symbol': 'FUSDT', 'price': 0.01571, 'life_time': 1761713288.889216}, {'side': 'ПОКУПКА', 'symbol': 'KERNELUSDT', 'price': 0.15, 'life_time': 1761713288.889318}, {'side': 'ПОКУПКА', 'symbol': 'TRUMPUSDT', 'price': 7.51, 'life_time': 1761713288.8891325}, {'side': 'ПОКУПКА', 'symbol': 'DEGOUSDT', 'price': 0.75, 'life_time': 1761713288.8863635}, {'side': 'ПОКУПКА', 'symbol': 'EULUSDT', 'price': 8.542, 'life_time': 1761713289.1913743}, {'side': 'ПОКУПКА', 'symbol': 'KMNOUSDT', 'price': 0.06305, 'life_time': 1761713289.2077384}, {'side': 'ПОКУПКА', 'symbol': 'PUMPUSDT', 'price': 0.00467, 'life_time': 1761713289.2082388}, {'side': 'ПОКУПКА', 'symbol': 'LQTYUSDT', 'price': 0.519, 'life_time': 1761713289.2084165}, {'side': 'ПОКУПКА', 'symbol': 'DASHUSDT', 'price': 45.5, 'life_time': 1761713289.2124922}, {'side': 'ПОКУПКА', 'symbol': 'AVNTUSDT', 'price': 0.6222, 'life_time': 1761713289.2125993}, {'side': 'ПОКУПКА', 'symbol': 'TURTLEUSDT', 'price': 0.141, 'life_time': 1761713289.2126405}, {'side': 'ПОКУПКА', 'symbol': 'ASTERUSDT', 'price': 1.079, 'life_time': 1761713289.511561}, {'side': 'ПОКУПКА', 'symbol': 'OGUSDT', 'price': 10.5, 'life_time': 1761713289.512141}, {'side': 'ПОКУПКА', 'symbol': 'BERAUSDT', 'price': 1.802, 'life_time': 1761713289.5124514}, {'side': 'ПОКУПКА', 'symbol': 'NXPCUSDT', 'price': 0.3715, 'life_time': 1761713289.5269818}, {'side': 'ПОКУПКА', 'symbol': 'VANAUSDT', 'price': 2.736, 'life_time': 1761713289.5270212}] [{'side': 'ПРОДАЖА', 'symbol': 'CHRUSDT', 'price': 0.1, 'life_time': 1761713288.8861706}, {'side': 'ПРОДАЖА', 'symbol': 'TRUMPUSDT', 'price': 7.8, 'life_time': 1761713288.889205}, {'side': 'ПРОДАЖА', 'symbol': 'DEGOUSDT', 'price': 1.088, 'life_time': 1761713288.8864167}, {'side': 'ПРОДАЖА', 'symbol': 'HAEDALUSDT', 'price': 0.1699, 'life_time': 1761713289.1899269}, {'side': 'ПРОДАЖА', 'symbol': 'KMNOUSDT', 'price': 0.06526, 'life_time': 1761713289.2077708}, {'side': 'ПРОДАЖА', 'symbol': 'PUMPUSDT', 'price': 0.005, 'life_time': 1761713289.2082758}, {'side': 'ПРОДАЖА', 'symbol': 'GIGGLEUSDT', 'price': 168.0, 'life_time': 1761713289.2084126}, {'side': 'ПРОДАЖА', 'symbol': 'DASHUSDT', 'price': 49.0, 'life_time': 1761713289.212561}, {'side': 'ПРОДАЖА', 'symbol': 'AVNTUSDT', 'price': 0.6449, 'life_time': 1761713289.2126284}, {'side': 'ПРОДАЖА', 'symbol': 'TURTLEUSDT', 'price': 0.1454, 'life_time': 1761713289.2126482}, {'side': 'ПРОДАЖА', 'symbol': 'ASTERUSDT', 'price': 1.082, 'life_time': 1761713289.5115662}, {'side': 'ПРОДАЖА', 'symbol': 'BERAUSDT', 'price': 1.856, 'life_time': 1761713289.512466}, {'side': 'ПРОДАЖА', 'symbol': 'ENSOUSDT', 'price': 1.9, 'life_time': 1761713289.5121338}, {'side': 'ПРОДАЖА', 'symbol': 'NXPCUSDT', 'price': 0.3769, 'life_time': 1761713289.5269995}, {'side': 'ПРОДАЖА', 'symbol': 'VANAUSDT', 'price': 2.775, 'life_time': 1761713289.527033}]