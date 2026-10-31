import aiohttp
import asyncio
import time
from typing import List, Dict, Optional
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
from utils.check_orderbook import check_orderbook
from config.params import KLINE_LIMIT_VOLUME, KLINE_LIMIT_1M, MEDIAN_VOLUME_CANDLES, MIN_MEDIAN_VOLUME_CANDLES

class DensityTracker:
    def __init__(self, max_concurrent=5):
        self.max_concurrent = max_concurrent
        self.session = None
    
    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()

    async def get_orderbook(self, symbol: str) -> tuple:
        """Асинхронное получение стакана"""
        url = "https://api.bybit.com/v5/market/orderbook"
        params = {"category": "spot", "symbol": symbol, "limit": 200}
        
        async with self.session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=5)) as response:
            data = await response.json()
            if data.get('retCode') == 0:
                asks = data['result'].get('a', [])
                bids = data['result'].get('b', [])
                return asks, bids
            return [], []

    async def get_klines_info(self, symbol: str, price_plotnost: float = None) -> tuple:
        """Асинхронное получение данных kline"""
        url = "https://api.bybit.com/v5/market/kline"
        params = {
            "category": "spot", 
            "symbol": symbol, 
            "interval": "5", 
            "limit": KLINE_LIMIT_VOLUME
        }
        
        async with self.session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=5)) as response:
            data = await response.json()
            
            if data.get('retCode') != 0:
                return 0, 0, 0, 0
            
            klines = data['result']['list']
            if len(klines) < MIN_MEDIAN_VOLUME_CANDLES + 2:
                return 0, 0, 0, 0
            
            # Объём предыдущей свечи в USDT (turnover)
            volume_last_kline = float(klines[1][6])
            volume_current_kline = float(klines[0][6])
            closed = klines[1:][:MEDIAN_VOLUME_CANDLES]
            sum_volume_klines = median_usdt_volume(volume[6] for volume in closed)

            avg_size_five_klines = median_tr_pct_from_klines(
                klines,
                newest_first=True,
                bar_count=MEDIAN_VOLUME_CANDLES,
                min_bars=MIN_MEDIAN_VOLUME_CANDLES,
            )
            
            # Текущая цена и расчет расстояния
            current_price = float(klines[0][4])
            candle_ts, candle_high, candle_low = current_candle_extra(klines[0])
            percent_range = abs((price_plotnost * 100) / current_price - 100) if price_plotnost else 0
            
            return percent_range, volume_last_kline, volume_current_kline, sum_volume_klines, avg_size_five_klines, current_price, candle_high, candle_low, candle_ts

    async def get_klines_1m_extremes(self, symbol: str) -> tuple:
        url = "https://api.bybit.com/v5/market/kline"
        params = {
            "category": "spot",
            "symbol": symbol,
            "interval": "1",
            "limit": KLINE_LIMIT_1M,
        }
        try:
            async with self.session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=5)) as response:
                data = await response.json()
                if data.get('retCode') != 0:
                    return 0, 0, 0
                klines = (data.get('result') or {}).get('list') or []
                return extremes_from_klines(klines, newest_first=True)
        except Exception:
            return 0, 0, 0

    async def process_density_batch(self, densities: List[Dict]) -> List[Dict]:
        """Параллельная обработка группы плотностей"""
        semaphore = asyncio.Semaphore(self.max_concurrent)
        
        async def process_single(density: Dict):
            async with semaphore:
                try:
                    return await self._process_single_density(density)
                except Exception as e:
                    print(f"Ошибка обработки {density['symbol']}: {e}")
                    return density
        
        tasks = [process_single(density) for density in densities]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Фильтруем валидные результаты
        return [r for r in results if r is not None and not isinstance(r, Exception)]

    async def _process_single_density(self, density: Dict) -> Optional[Dict]:
        """Обработка одной плотности"""
        current_time = time.time()
        symbol = density["symbol"]
        target_price = density["price"]
        side = density['side']
        
        # Параллельно: стакан, 5m и 1m
        orderbook_task = self.get_orderbook(symbol)
        kline_task = self.get_klines_info(symbol, target_price)
        kline_1m = None
        if should_watch_1m(density):
            asks_bids, kline_data, kline_1m = await asyncio.gather(
                orderbook_task, kline_task, self.get_klines_1m_extremes(symbol)
            )
        else:
            asks_bids, kline_data = await asyncio.gather(orderbook_task, kline_task)
        asks, bids = asks_bids
        percent, volume_last_kline, volume_current_kline, sum_volume_klines, avg_size_five_klines, current_price, candle_high, candle_low, candle_ts = unpack_kline_info(kline_data)

        if not bids and not asks:
            return density

        if side == 'ПОКУПКА':
            orders_to_check = bids
            long = True
        else:
            orders_to_check = asks
            long = False

        for order in orders_to_check:
            order_price = float(order[0])
            qty_size = float(order[1])
            order_value = order_price * qty_size

            if order_price == target_price and is_density_wall(order_price, qty_size, sum_volume_klines):
                flag_zapolnennosti = check_orderbook(bids, asks, target_price, qty_size, percent, long)

                if flag_zapolnennosti:
                    print(f"Плотность {symbol} найдена, но стакан заполнен")
                    return None

                first_seen_key = 'bid_first_seen' if side == 'ПОКУПКА' else 'ask_first_seen'
                if first_seen_key not in density:
                    density[first_seen_key] = current_time

                density['life_time'] = current_time - density[first_seen_key]

                if volume_last_kline == 0:
                    print(f"Предупреждение: volume_last_kline = 0 для {density['symbol']}")
                    volume_last_kline = 0.001
                if volume_current_kline == 0:
                    print(f"Предупреждение: volume_current_kline = 0 для {density['symbol']}")
                    volume_current_kline = 0.001
                if sum_volume_klines == 0:
                    print(f"Предупреждение: sum_volume_klines = 0 для {density['symbol']}")
                    volume_current_kline = 0.001

                otnosh_volume_last_kline = order_value / volume_last_kline
                otnosh_volume_current_kline = order_value / volume_current_kline
                otnosh_sum_volume_klines = order_value / sum_volume_klines

                density["exchange"] = "BYBIT"
                density["qty_size"] = qty_size
                density["order_size"] = order_value
                density["volume_last_kline"] = volume_last_kline
                density['otnosh_volume_last_kline'] = otnosh_volume_last_kline
                density['volume_current_kline'] = volume_current_kline
                density['otnosh_volume_current_kline'] = otnosh_volume_current_kline
                density['otnosh_avg_volume_klines'] = otnosh_sum_volume_klines
                density["avg_size_five_klines"] = avg_size_five_klines
                density["percent"] = round(percent, 2)
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

        return None

# Оптимизированная основная функция
async def update_plotnost_time(list_bids: List[Dict], list_asks: List[Dict]) -> List[Dict]:
    """Оптимизированная версия отслеживания времени жизни плотностей"""
    try:
        list_densities = list_bids + list_asks
        
        async with DensityTracker(max_concurrent=5) as tracker:
            results = await tracker.process_density_batch(list_densities)
        
        print(f"Активных плотностей: {len(results)}")
        print(results)
        return results
        
    except Exception as e:
        print(f"Ошибка в get_life_time_plotnost_optimized: {e}")
        return list(list_bids or []) + list(list_asks or [])