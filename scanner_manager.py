import asyncio
import time
from config.params import is_ignored_symbol
from utils.density_store import load_exchange, save_exchange
from utils.price_step import refresh_price_steps
from utils.outcome_journal import (
    apply_touch_eaten,
    density_key,
    record_eaten_attack_outcome,
    record_touch_outcome,
    is_bid_side,
    record_bounce_outcome,
    record_fut_bounce_outcome,
    record_outcome,
    stamp_last_checked,
    update_peaks,
)

class ExchangeScanner:
    def __init__(self, name, get_symbols_func, analyze_orderbooks_func, update_densities_func):
        self.name = name
        self.get_symbols_func = get_symbols_func
        self.analyze_orderbooks_func = analyze_orderbooks_func
        self.update_densities_func = update_densities_func
        
        self.symbols = []
        self.active_bids = []
        self.active_asks = []
        self.last_symbols_update = 0
        self.last_orderbooks_update = 0
        self.last_density_update = 0
        self.is_running = True
        self._closed_keys = set()

    async def run(self):
        """Основной цикл сканирования для одной биржи"""
        self.restore()
        print(f"🚀 Запуск сканера {self.name}...")
        try:
            await refresh_price_steps(self.name, self.symbols)
        except Exception as error:
            print(f"⚠️ {self.name}: шаг цены не загружен - {error}")
        
        while self.is_running:
            try:
                current_time = time.time()
                
                # 1. Обновление символов (раз в час)
                if current_time - self.last_symbols_update >= 3600 or not self.symbols:
                    await self._update_symbols()
                
                # 2. Анализ стаканов (раз в 5 минут)
                if current_time - self.last_orderbooks_update >= 300 and self.symbols:
                    await self._analyze_orderbooks()
                
                # 3. Проверка плотностей (раз в минуту)
                if time.time() - self.last_density_update >= 60:
                    await self._update_densities()
                
                await asyncio.sleep(10)
                
            except Exception as e:
                print(f"❌ {self.name}: Ошибка - {e}")
                await asyncio.sleep(30)

    async def _update_symbols(self):
        """Обновление списка символов"""
        print(f"🔍 {self.name}: Обновляем символы...")
        try:
            new_symbols = [
                symbol for symbol in (await self.get_symbols_func() or [])
                if not is_ignored_symbol(symbol)
            ]
            print(f"✅ {self.name}: Получено {len(new_symbols)} символов")
            
            # Очистка устаревших плотностей
            previous = self.active_bids + self.active_asks
            self.active_bids = [
                p for p in self.active_bids
                if p.get('symbol') in new_symbols and not is_ignored_symbol(p.get('symbol'))
            ]
            self.active_asks = [
                p for p in self.active_asks
                if p.get('symbol') in new_symbols and not is_ignored_symbol(p.get('symbol'))
            ]
            await self._close_gone(previous, self.active_bids + self.active_asks)
            
            self.symbols = new_symbols
            self.last_symbols_update = time.time()
            await refresh_price_steps(self.name, new_symbols)
            self._save()
            
        except Exception as e:
            print(f"❌ {self.name}: Ошибка обновления символов - {e}")

    async def _analyze_orderbooks(self):
        """Анализ стаканов"""
        print(f"📊 {self.name}: Анализируем стаканы...")
        try:
            bids, asks = await self.analyze_orderbooks_func(self.symbols)
            bids = [p for p in (bids or []) if not is_ignored_symbol(p.get('symbol'))]
            asks = [p for p in (asks or []) if not is_ignored_symbol(p.get('symbol'))]
            
            # Обновление активных плотностей
            await self._update_active_densities(self.active_bids, bids, 'bids')
            await self._update_active_densities(self.active_asks, asks, 'asks')
            
            self.last_orderbooks_update = time.time()
            print(f"✅ {self.name}: Найдено bids={len(bids)}, asks={len(asks)}")
            
        except Exception as e:
            print(f"❌ {self.name}: Ошибка анализа стаканов - {e}")

    async def _update_densities(self):
        """Обновление плотностей"""
        if self.active_bids or self.active_asks:
            print(f"⏰ {self.name}: Проверяем плотности...")
            try:
                active_densities = await self.update_densities_func(self.active_bids, self.active_asks)
                active_densities = [
                    density for density in (active_densities or [])
                    if not is_ignored_symbol(density.get('symbol'))
                ]
                self.last_density_update = time.time()
                previous = self.active_bids + self.active_asks
                await self._close_gone(previous, active_densities)
                self._set_active(active_densities)
                await self._flush_bounce_records(self.active_bids + self.active_asks)

                print(f"✅ {self.name}: Активных плотностей: {len(active_densities)}")
                
            except Exception as e:
                print(f"❌ {self.name}: Ошибка обновления плотностей - {e}")
        else:
            self.last_density_update = time.time()

    def _stamp_exchange(self, density):
        if density is not None:
            density['exchange'] = self.name

    async def _update_active_densities(self, active_list, new_list, list_type):
        """Обновление списка активных плотностей"""
        # Ваша логика обновления из main.py
        current_map = {density_key(p): p for p in active_list}
        new_map = {density_key(p): p for p in new_list}
        
        sticky_keys = (
            'percent', 'current_price', 'candle_high', 'candle_low',
            'candle_open_ts', 'min_percent', 'max_bounce', 'max_bounce_price',
            'max_bounce_armed', 'max_fut_bounce', 'max_fut_bounce_armed',
            'last_bounce_move',
            'last_bounce_price',
            'bounce_moves', 'bounce_rows_written', 'touch_rows_written',
            'bounce_prices',
            'touch_count', 'bounce_count', 'bounce_armed', 'bounce_ref_price',
            'touch_active', 'touch_bounced',
            'touch_min_percent', 'touch_candle_ts', 'touch_in_zone', 'survived_attack',
            'last_touch_price', 'last_touch_percent', 'max_order_size', 'peak_order_size',
            'found_percent', 'journal_last_percent', 'journal_last_fut_percent',
            'wall_id', 'first_touch_ts', 'last_touch_ts',
            'episode_btc_touch',
            'spot_tick_pct', 'fut_tick_pct',
            'last_bounce_delay_min', 'bounce_delays',
            'last_fut_bounce_delay_min', 'fut_bounce_delays',
            'last_checked_ts',
            'watch_1m', 'candle_1m_high', 'candle_1m_low', 'candle_1m_ts',
            'candle_1m_bar_high', 'candle_1m_bar_low', 'candle_1m_close',
            'm1_ext_high', 'm1_ext_low', 'm1_episode_high', 'm1_episode_low',
            'candle_fut_1m_high', 'candle_fut_1m_low', 'candle_fut_1m_ts',
            'candle_fut_1m_bar_high', 'candle_fut_1m_bar_low', 'candle_fut_1m_close',
            'fut_episode_high', 'fut_episode_low',
            'fut_touch_price', 'fut_touch_candle_ts', 'fut_touch_pending', 'fut_bounce_armed',
            'fut_adverse_pct', 'fut_adverse_price',
            'last_fut_adverse_pct', 'last_fut_adverse_price',
            'spot_adverse_pct', 'spot_adverse_price',
            'last_spot_adverse_pct', 'last_spot_adverse_price',
            'fut_bounce_count', 'fut_bounce_rows_written',
            'last_fut_bounce_move', 'last_fut_bounce_price',
            'fut_bounce_moves', 'fut_bounce_prices',
            'eaten_ratio', 'remaining_pct', 'eaten_traded_usdt', 'last_eaten_ts',
            'last_touch_traded_usdt', 'last_touch_eaten_ratio',
            'eaten_attack_count', 'eaten_attack_rows_written',
            'touch_eaten_pending',
            'price_beyond_wall', 'price_beyond_episode',
            'first_seen_ts', 'bid_first_seen', 'ask_first_seen',
            'qty_size', 'order_size', 'volume_last_kline', 'volume_current_kline',
            'otnosh_volume_last_kline', 'otnosh_volume_current_kline',
            'otnosh_avg_volume_klines', 'avg_size_five_klines',
            'spot_tick_pct', 'fut_tick_pct',
        )
        updated_list = []
        for key, existing in current_map.items():
            if key in new_map:
                saved = {k: existing[k] for k in sticky_keys if k in existing}
                old_life = existing.get('life_time')
                existing.update(new_map[key])
                existing.update(saved)
                if old_life is not None and old_life < 1e9:
                    existing['life_time'] = old_life
                self._stamp_exchange(existing)
                update_peaks(existing)
                await apply_touch_eaten(self.name, existing)
                stamp_last_checked(existing)
                updated_list.append(existing)
        
        for key, new_item in new_map.items():
            if key not in current_map:
                self._closed_keys.discard(key)
                self._stamp_exchange(new_item)
                update_peaks(new_item)
                await apply_touch_eaten(self.name, new_item)
                stamp_last_checked(new_item)
                updated_list.append(new_item)
        
        await self._close_gone(list(active_list), updated_list)
        await self._flush_bounce_records(updated_list)
        active_list.clear()
        active_list.extend(updated_list)
        self._save()

    def _set_active(self, densities):
        bids = []
        asks = []
        for density in densities:
            self._stamp_exchange(density)
            update_peaks(density)
            stamp_last_checked(density)
            self._closed_keys.discard(density_key(density))
            if is_bid_side(density):
                bids.append(density)
            else:
                asks.append(density)
        self.active_bids = bids
        self.active_asks = asks
        self._save()

    def restore(self):
        pack = load_exchange(self.name)
        self.symbols = [symbol for symbol in pack['symbols'] if not is_ignored_symbol(symbol)]
        self.active_bids = [p for p in pack['bids'] if not is_ignored_symbol(p.get('symbol'))]
        self.active_asks = [p for p in pack['asks'] if not is_ignored_symbol(p.get('symbol'))]
        for density in self.active_bids + self.active_asks:
            self._stamp_exchange(density)
            update_peaks(density)
            density.pop('bounce_just_happened', None)
            density.pop('fut_bounce_just_happened', None)
        count = len(self.active_bids) + len(self.active_asks)
        if count or self.symbols:
            now = time.time()
            self.last_symbols_update = now if self.symbols else 0
            self.last_orderbooks_update = now
            self.last_density_update = 0
            print(
                f"📂 {self.name}: восстановлено стен={count} "
                f"(bids={len(self.active_bids)}, asks={len(self.active_asks)}, "
                f"монет={len(self.symbols)})"
            )
        self._save()

    def _save(self):
        try:
            save_exchange(self.name, self.symbols, self.active_bids, self.active_asks)
        except Exception as e:
            print(f"⚠️ {self.name}: Не удалось сохранить плотности - {e}")

    async def _flush_bounce_records(self, densities):
        for density in densities or []:
            self._stamp_exchange(density)
            density.pop('bounce_just_happened', False)
            density.pop('fut_bounce_just_happened', False)
            try:
                await apply_touch_eaten(self.name, density)
            except Exception as e:
                print(f"⚠️ {self.name}: Ошибка разбора подхода - {e}")
            try:
                await record_eaten_attack_outcome(self.name, density)
            except Exception as e:
                print(f"⚠️ {self.name}: Ошибка журнала разъедания - {e}")
            try:
                await record_touch_outcome(self.name, density)
            except Exception as e:
                print(f"⚠️ {self.name}: Ошибка журнала подхода - {e}")
            try:
                await record_bounce_outcome(self.name, density)
            except Exception as e:
                print(f"⚠️ {self.name}: Ошибка журнала отскока - {e}")
            try:
                await record_fut_bounce_outcome(self.name, density)
            except Exception as e:
                print(f"⚠️ {self.name}: Ошибка журнала фьюч-отскока - {e}")

    async def _close_gone(self, previous, alive):
        alive_keys = {density_key(density) for density in alive}
        for density in previous:
            key = density_key(density)
            if key in alive_keys or key in self._closed_keys:
                continue
            if is_ignored_symbol(density.get('symbol')):
                self._closed_keys.add(key)
                continue
            self._closed_keys.add(key)
            try:
                self._stamp_exchange(density)
                await record_outcome(self.name, density)
            except Exception as e:
                print(f"⚠️ {self.name}: Ошибка журнала исходов - {e}")

    def stop(self):
        """Остановка сканера"""
        self.is_running = False
        self._save()