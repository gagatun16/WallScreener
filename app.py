# app.py - ПОЛНЫЙ КОД С СИНХРОНИЗИРОВАННЫМИ ГРАФИКАМИ
from flask import Flask, jsonify, request, render_template, send_file, abort
from flask_cors import CORS
import asyncio
import threading
import time
from datetime import datetime
import os
import signal
import sys
import aiohttp
import hmac
from hashlib import sha256
from urllib.parse import urlparse
import pandas as pd
import base64
import io
import matplotlib
matplotlib.use('Agg')  # Важно для использования matplotlib в Flask
import matplotlib.pyplot as plt
import mplfinance as mpf

_chart_lock = threading.Lock()
from config.params import *
from config.api_keys import APIKEY, SECRETKEY
from utils.outcome_journal import read_journal, _life_time_sec
from utils.outcome_chart import chart_path

# Импортируем сканеры из существующего кода
from scanner_manager import ExchangeScanner
from main import SCANNERS  # Используем сканеры из main

app = Flask(__name__)
CORS(app)

class FlaskScannerManager:
    def __init__(self):
        self.scanners = []
        self.tasks = []
        self.is_running = False
        self.scanner_thread = None
        self.all_densities = {}  # Хранилище для плотностей всех бирж
        self.last_update_time = {}
        
        # Создаем экземпляры сканеров
        for scanner_config in SCANNERS:
            scanner = ExchangeScanner(
                name=scanner_config.name,
                get_symbols_func=scanner_config.get_symbols_func,
                analyze_orderbooks_func=scanner_config.analyze_orderbooks_func,
                update_densities_func=scanner_config.update_densities_func
            )
            self.scanners.append(scanner)
            scanner.restore()
            self.all_densities[scanner.name] = {
                'bids': [],
                'asks': [],
                'last_update': 'Нет данных'
            }
            self.last_update_time[scanner.name] = 0
    
    def start_background_scanner(self):
        """Запуск сканера в фоновом потоке"""
        if self.scanner_thread and self.scanner_thread.is_alive():
            return
        
        self.is_running = True
        self.scanner_thread = threading.Thread(target=self._run_scanner_loop, daemon=True)
        self.scanner_thread.start()
        print("✅ Фоновый сканер запущен")
    
    def _run_scanner_loop(self):
        """Основной цикл сканера в отдельном потоке"""
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        async def scanner_wrapper():
            """Обертка для запуска всех сканеров"""
            scanner_tasks = []
            for scanner in self.scanners:
                task = asyncio.create_task(self._monitor_scanner(scanner))
                scanner_tasks.append(task)
            
            await asyncio.gather(*scanner_tasks, return_exceptions=True)
        
        try:
            loop.run_until_complete(scanner_wrapper())
        except Exception as e:
            print(f"❌ Ошибка в сканере: {e}")
        finally:
            loop.close()
    
    async def _monitor_scanner(self, scanner):
        """Мониторинг одного сканера и сбор данных"""
        print(f"🚀 Мониторинг сканера {scanner.name}...")
        await self._update_web_data(scanner)

        while self.is_running:
            try:
                current_time = time.time()
                
                # 1. Обновление символов (раз в час)
                if current_time - scanner.last_symbols_update >= 3600 or not scanner.symbols:
                    await self._safe_update_symbols(scanner)
                
                # 2. Анализ стаканов (раз в 5 минут)
                if current_time - scanner.last_orderbooks_update >= 300 and scanner.symbols:
                    await self._safe_analyze_orderbooks(scanner)
                
                # 3. Проверка плотностей (раз в минуту)
                if time.time() - scanner.last_density_update >= 60:
                    await self._safe_update_densities(scanner)
                
                # 4. Обновление данных для веб-интерфейса
                await self._update_web_data(scanner)
                
                await asyncio.sleep(10)
                
            except Exception as e:
                print(f"❌ {scanner.name}: Ошибка в мониторинге - {e}")
                await asyncio.sleep(30)
    
    async def _safe_update_symbols(self, scanner):
        """Безопасное обновление символов"""
        try:
            await scanner._update_symbols()
        except Exception as e:
            print(f"❌ {scanner.name}: Ошибка обновления символов - {e}")
    
    async def _safe_analyze_orderbooks(self, scanner):
        """Безопасный анализ стаканов"""
        try:
            await scanner._analyze_orderbooks()
        except Exception as e:
            print(f"❌ {scanner.name}: Ошибка анализа стаканов - {e}")
    
    async def _safe_update_densities(self, scanner):
        """Безопасное обновление плотностей"""
        try:
            await scanner._update_densities()
        except Exception as e:
            print(f"❌ {scanner.name}: Ошибка обновления плотностей - {e}")
    
    async def _update_web_data(self, scanner):
        """Обновление данных для веб-интерфейса: все живые плотности без фильтра сигнала"""
        try:
            all_bids = []
            all_asks = []
            all_densities = []

            for bid in scanner.active_bids:
                bid['_side'] = 'bid'
                all_densities.append(bid)

            for ask in scanner.active_asks:
                ask['_side'] = 'ask'
                all_densities.append(ask)

            for density in all_densities:
                if is_ignored_symbol(density.get('symbol')):
                    continue
                if _life_time_sec(density) < MIN_LIFE_TIME_SITE:
                    continue
                formatted_density = await self._format_density_data(density, scanner.name)
                if density['_side'] == 'bid':
                    all_bids.append(formatted_density)
                else:
                    all_asks.append(formatted_density)

            all_bids.sort(key=lambda x: x['life_time'], reverse=True)
            all_asks.sort(key=lambda x: x['life_time'], reverse=True)
            
            # Обновляем хранилище
            self.all_densities[scanner.name] = {
                'bids': all_bids,
                'asks': all_asks,
                'last_update': datetime.now().strftime('%H:%M:%S'),
                'total_filtered': len(all_densities),
                'total_all': len(all_densities)
            }
            self.last_update_time[scanner.name] = time.time()
            
            print(f"📊 {scanner.name}: Обновлено - bids: {len(all_bids)}, asks: {len(all_asks)}")
            
        except Exception as e:
            print(f"❌ {scanner.name}: Ошибка обновления веб-данных - {e}")

    async def _format_density_data(self, density, exchange_name):
        """Форматирование данных плотности для веб-интерфейса"""
        try:
            # Преобразуем время жизни из секунд в минуты
            life_time_seconds = _life_time_sec(density)
            try:
                life_time_minutes = int(float(life_time_seconds) / 60)
            except:
                life_time_minutes = 0
            
            volatility = self._safe_float(density.get('avg_size_five_klines', 0))

            formatted = {
                'exchange': exchange_name,
                'side': density.get('_side', 'bid'),
                'symbol': density.get('symbol', 'N/A'),
                'price': self._format_number(density.get('price', 0), 6),
                'price_raw': self._safe_float(density.get('price', 0)),
                'qty_size': self._format_number(density.get('qty_size', 0), 2),
                'order_size': self._format_number(density.get('order_size', 0), 2),
                'percent': self._format_number(density.get('percent', 0), 2),
                'percent_raw': self._safe_float(density.get('percent', 100)),
                'min_percent': self._format_number(density.get('min_percent', density.get('percent', 0)), 2),
                'max_bounce': self._format_number(density.get('max_bounce', 0), 2),
                'touch_count': int(float(density.get('touch_count') or 0)),
                'bounce_count': int(float(density.get('bounce_count') or 0)),
                'life_time': life_time_minutes,
                'volume_last_kline': self._format_number(density.get('volume_last_kline', 0), 2),
                'avg_size_five_klines': self._format_number(volatility, 2),
                'order_size_raw': self._safe_float(density.get('order_size', 0)),
                'eaten_ratio': self._format_number(density.get('eaten_ratio', ''), 4) if density.get('eaten_ratio') not in (None, '') else '',
                'remaining_pct': self._format_number(density.get('remaining_pct', ''), 2) if density.get('remaining_pct') not in (None, '') else '',
                'volume_ratio_avg': self._format_number(density.get('otnosh_avg_volume_klines', ''), 4) if density.get('otnosh_avg_volume_klines') not in (None, '') else '',
                'volume_ratio_last': self._format_number(density.get('otnosh_volume_last_kline', ''), 4) if density.get('otnosh_volume_last_kline') not in (None, '') else '',
                'first_seen': density.get('first_seen', '-'),
                'last_updated': density.get('last_updated', '-'),
            }
                
            return formatted
        except Exception as e:
            print(f"❌ Ошибка форматирования данных: {e}")
            return {
                'exchange': exchange_name,
                'side': density.get('_side', 'bid'),
                'symbol': 'ERROR',
                'price': '0',
                'price_raw': 0,
                'qty_size': '0',
                'order_size': '0',
                'percent': '0',
                'percent_raw': 100,
                'life_time': '0',
                'volume_last_kline': '0',
                'avg_size_five_klines': '0',
                'order_size_raw': 0,
                'first_seen': '-',
                'last_updated': '-',
            }

    def _safe_float(self, value):
        """Безопасное преобразование в float"""
        try:
            if isinstance(value, (int, float)):
                return float(value)
            elif isinstance(value, str):
                clean_str = value.replace(',', '.').replace(' ', '').replace('%', '')
                return float(clean_str)
            else:
                return 0.0
        except:
            return 0.0
    
    def _format_number(self, value, decimals=2):
        """Форматирование числа для отображения"""
        try:
            if isinstance(value, (int, float)):
                num = float(value)
            else:
                num = float(str(value).replace(',', '.'))
            
            if num == 0:
                return '0'
            
            if num >= 1000:
                return f"{num:,.{decimals}f}"
            elif num >= 1:
                return f"{num:.{decimals}f}"
            else:
                return f"{num:.{decimals+2}f}".rstrip('0').rstrip('.')
        except:
            return str(value)
    
    def stop(self):
        """Остановка всех сканеров"""
        self.is_running = False
        for scanner in self.scanners:
            scanner.stop()
        print("🛑 Все сканеры остановлены, активные плотности сохранены")
    
    def get_all_densities(self):
        """Получение всех плотностей для API"""
        return self.all_densities

# Создаем глобальный менеджер сканеров
scanner_manager = FlaskScannerManager()

# ====================== КЛАСС ДЛЯ ГЕНЕРАЦИИ ГРАФИКОВ ======================

class UniversalChartGenerator:
    """Универсальный генератор графиков для всех бирж с автоматическим определением формата"""
    
    @staticmethod
    def _kline_params(exchange_upper: str, interval: str) -> tuple:
        """Параметр таймфрейма API: (bybit, okx bar, bitget granularity, binance-like)."""
        tf = '1m' if str(interval).lower().startswith('1') else '5m'
        if tf == '1m':
            return '1', '1m', '1min', '1m'
        return '5', '5m', '5min', '5m'

    @staticmethod
    def _is_futures_market(market) -> bool:
        return str(market or 'spot').lower() in ('futures', 'fut', 'swap', 'perp', 'perpetual')

    @staticmethod
    def _perp_symbol(exchange_upper: str, symbol: str) -> str:
        raw = str(symbol or '')
        if exchange_upper == 'OKX':
            return raw if raw.upper().endswith('-SWAP') else f'{raw}-SWAP'
        if exchange_upper == 'MEXC':
            if '_' in raw:
                return raw
            upper = raw.upper()
            if upper.endswith('USDT') and len(upper) > 4:
                return raw[:-4] + '_USDT'
            return raw
        return raw

    @staticmethod
    def _bingx_kline_request(path: str, symbol: str, interval: str, limit: int):
        params = {
            'symbol': symbol,
            'interval': interval,
            'limit': str(limit),
            'timestamp': str(int(time.time() * 1000)),
        }
        query_string = '&'.join(f'{key}={value}' for key, value in params.items())
        signature = hmac.new(
            SECRETKEY.encode('utf-8'),
            query_string.encode('utf-8'),
            digestmod=sha256,
        ).hexdigest()
        url = f'https://open-api.bingx.com{path}?{query_string}&signature={signature}'
        return url, {'X-BX-APIKEY': APIKEY}

    @staticmethod
    def _kline_host_ok(exchange_upper: str, is_futures: bool, url: str) -> bool:
        """Свечи только со своей биржи: BingX никогда не уходит на Binance."""
        host = (urlparse(url).hostname or '').lower()
        expected = {
            'BINANCE': 'fapi.binance.com' if is_futures else 'api.binance.com',
            'BINGX': 'open-api.bingx.com',
            'BITGET': 'api.bitget.com',
            'BYBIT': 'api.bybit.com',
            'MEXC': 'contract.mexc.com' if is_futures else 'api.mexc.com',
            'OKX': 'www.okx.com',
        }.get(exchange_upper)
        if not expected:
            print(f'⚠️ Нет свечного API для биржи {exchange_upper}')
            return False
        if host != expected:
            print(f'⚠️ Свечи {exchange_upper} {("futures" if is_futures else "spot")} с {host}, нужен {expected}')
            return False
        return True

    @staticmethod
    async def get_candles_safe(
        exchange: str,
        symbol: str,
        interval: str = "5m",
        limit: int = 100,
        market: str = "spot",
    ) -> pd.DataFrame:
        """Свечи спота или USDT-перпетуала строго с API этой биржи."""
        exchange_upper = str(exchange or '').strip().upper()
        bybit_tf, okx_bar, bitget_tf, binance_tf = UniversalChartGenerator._kline_params(exchange_upper, interval)
        is_futures = UniversalChartGenerator._is_futures_market(market)

        try:
            async with aiohttp.ClientSession() as session:
                async def fetch_json(url, params=None, headers=None):
                    if not UniversalChartGenerator._kline_host_ok(exchange_upper, is_futures, url):
                        return None
                    async with session.get(url, params=params, headers=headers) as response:
                        return await response.json()

                if exchange_upper == 'BYBIT':
                    data = await fetch_json(
                        "https://api.bybit.com/v5/market/kline",
                        params={
                            "category": "linear" if is_futures else "spot",
                            "symbol": symbol,
                            "interval": bybit_tf,
                            "limit": limit,
                        },
                    )
                    if not data or data.get('retCode') != 0:
                        print(f"Bybit API error: {(data or {}).get('retMsg')}")
                        return None
                    return UniversalChartGenerator._parse_bybit_data(data['result']['list'])

                if exchange_upper == 'OKX':
                    inst = UniversalChartGenerator._perp_symbol('OKX', symbol) if is_futures else symbol
                    data = await fetch_json(
                        "https://www.okx.com/api/v5/market/candles",
                        params={"instId": inst, "bar": okx_bar, "limit": str(limit)},
                    )
                    if not data or data.get('code') != "0":
                        print(f"OKX API error: {(data or {}).get('msg')}")
                        return None
                    return UniversalChartGenerator._parse_okx_data(data['data'])

                if exchange_upper == 'BITGET':
                    if is_futures:
                        url = "https://api.bitget.com/api/v2/mix/market/candles"
                        params = {
                            "symbol": symbol,
                            "productType": "USDT-FUTURES",
                            "granularity": binance_tf,
                            "limit": str(limit),
                        }
                    else:
                        url = "https://api.bitget.com/api/v2/spot/market/candles"
                        params = {
                            "symbol": symbol,
                            "granularity": bitget_tf,
                            "limit": str(limit),
                        }
                    data = await fetch_json(url, params=params)
                    if not data or data.get('code') != "00000":
                        print(f"Bitget API error: {(data or {}).get('msg')}")
                        return None
                    return UniversalChartGenerator._parse_bitget_data(data['data'])

                if exchange_upper == 'BINGX':
                    path = (
                        '/openApi/swap/v3/quote/klines' if is_futures
                        else '/openApi/spot/v2/market/kline'
                    )
                    url, headers = UniversalChartGenerator._bingx_kline_request(
                        path, symbol, binance_tf, limit,
                    )
                    payload = await fetch_json(url, headers=headers)
                    if payload is None:
                        return None
                    if isinstance(payload, dict):
                        code = payload.get('code')
                        if code not in (0, '0', None):
                            print(f"BingX API error: {payload.get('msg') or code}")
                            return None
                        payload = payload.get('data')
                    return UniversalChartGenerator._parse_ohlcv_any(payload)

                if exchange_upper == 'MEXC':
                    if is_futures:
                        inst = UniversalChartGenerator._perp_symbol('MEXC', symbol)
                        iv = 'Min1' if binance_tf == '1m' else 'Min5'
                        url = f"https://contract.mexc.com/api/v1/contract/kline/{inst}"
                        payload = await fetch_json(url, params={"interval": iv})
                        data = payload.get('data') if isinstance(payload, dict) else payload
                        parsed = UniversalChartGenerator._parse_ohlcv_any(data)
                        if parsed is not None and len(parsed) > limit:
                            return parsed.iloc[-limit:]
                        return parsed
                    data = await fetch_json(
                        "https://api.mexc.com/api/v3/klines",
                        params={"symbol": symbol, "interval": binance_tf, "limit": limit},
                    )
                    return UniversalChartGenerator._parse_generic_data(data)

                if exchange_upper == 'BINANCE':
                    url = (
                        "https://fapi.binance.com/fapi/v1/klines" if is_futures
                        else "https://api.binance.com/api/v3/klines"
                    )
                    data = await fetch_json(
                        url,
                        params={"symbol": symbol, "interval": binance_tf, "limit": limit},
                    )
                    return UniversalChartGenerator._parse_generic_data(data)

                print(f'⚠️ Нет свечного API для биржи {exchange}')
                return None

        except Exception as e:
            print(f"Ошибка {exchange} {market} для {symbol}: {e}")
            return None
    
    @staticmethod
    def _parse_generic_data(data):
        """Парсинг данных в общем формате (Binance/MEXC/BingX)"""
        if not data or not isinstance(data, list):
            return None
        
        try:
            # Автоматически определяем количество колонок
            first_row = data[0] if data else []
            num_columns = len(first_row)
            
            # Создаем DataFrame в зависимости от формата
            if num_columns == 12:
                # Binance формат (12 колонок)
                df = pd.DataFrame(data, columns=[
                    "Open time", "Open", "High", "Low", "Close", "Volume",
                    "Close time", "Quote volume", "Trades", "Taker buy base", "Taker buy quote", "Ignore"
                ])
            elif num_columns == 8:
                # MEXC формат (8 колонок)
                df = pd.DataFrame(data, columns=[
                    "Open time", "Open", "High", "Low", "Close", "Volume",
                    "Close time", "Quote volume"
                ])
            elif num_columns == 6:
                # Упрощенный формат (6 колонок)
                df = pd.DataFrame(data, columns=[
                    "Open time", "Open", "High", "Low", "Close", "Volume"
                ])
            else:
                # Пробуем получить хотя бы основные колонки
                print(f"Неизвестный формат: {num_columns} колонок")
                # Используем только первые 6 колонок
                df = pd.DataFrame([row[:6] for row in data], 
                                 columns=["Open time", "Open", "High", "Low", "Close", "Volume"])
            
            # Конвертируем timestamp
            df["Open time"] = pd.to_datetime(df["Open time"], unit="ms", errors='coerce')
            df.set_index("Open time", inplace=True)
            
            # Конвертируем числовые колонки
            numeric_cols = ["Open", "High", "Low", "Close", "Volume"]
            for col in numeric_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # Оставляем только нужные колонки и сортируем
            available_cols = [col for col in numeric_cols if col in df.columns]
            if available_cols:
                df = df[available_cols]
                return df.sort_index().dropna()
            
            return None
            
        except Exception as e:
            print(f"Ошибка парсинга данных: {e}")
            return None

    @staticmethod
    def _parse_kline_dicts(rows):
        """Свечи списком словарей (BingX swap и похожие)."""
        if not rows or not isinstance(rows, list) or not isinstance(rows[0], dict):
            return None
        try:
            def pick(row, keys):
                lower = {str(key).lower(): value for key, value in row.items()}
                for key in keys:
                    if key.lower() in lower:
                        return lower[key.lower()]
                return None

            times, opens, highs, lows, closes, volumes = [], [], [], [], [], []
            for row in rows:
                times.append(pick(row, ('time', 't', 'openTime', 'timestamp', 'ts')))
                opens.append(pick(row, ('open', 'o', 'openPrice')))
                highs.append(pick(row, ('high', 'h', 'highPrice')))
                lows.append(pick(row, ('low', 'l', 'lowPrice')))
                closes.append(pick(row, ('close', 'c', 'closePrice')))
                volumes.append(pick(row, ('volume', 'vol', 'v', 'baseVol')))
            sample = float(times[0])
            unit = 'ms' if sample > 1e12 else 's'
            df = pd.DataFrame({
                'Open': opens, 'High': highs, 'Low': lows, 'Close': closes, 'Volume': volumes,
            }, index=pd.to_datetime(times, unit=unit, errors='coerce'))
            for col in ('Open', 'High', 'Low', 'Close', 'Volume'):
                df[col] = pd.to_numeric(df[col], errors='coerce')
            return df.sort_index().dropna()
        except Exception as e:
            print(f"Ошибка парсинга dict-свечей: {e}")
            return None

    @staticmethod
    def _parse_mexc_contract(data):
        """MEXC futures: {time, open, high, low, close, vol}."""
        if not isinstance(data, dict):
            return None
        times = data.get('time') or data.get('t')
        if not times:
            return None
        try:
            sample = float(times[0])
            unit = 'ms' if sample > 1e12 else 's'
            df = pd.DataFrame({
                'Open': data.get('open') or data.get('o'),
                'High': data.get('high') or data.get('h'),
                'Low': data.get('low') or data.get('l'),
                'Close': data.get('close') or data.get('c'),
                'Volume': data.get('vol') or data.get('volume') or data.get('v'),
            }, index=pd.to_datetime(times, unit=unit, errors='coerce'))
            for col in ('Open', 'High', 'Low', 'Close', 'Volume'):
                df[col] = pd.to_numeric(df[col], errors='coerce')
            return df.sort_index().dropna()
        except Exception as e:
            print(f"Ошибка парсинга MEXC contract: {e}")
            return None

    @staticmethod
    def _parse_ohlcv_any(data):
        if isinstance(data, dict):
            return UniversalChartGenerator._parse_mexc_contract(data)
        if isinstance(data, list) and data and isinstance(data[0], dict):
            return UniversalChartGenerator._parse_kline_dicts(data)
        return UniversalChartGenerator._parse_generic_data(data)
    
    @staticmethod
    def _parse_bybit_data(klines):
        """Парсинг данных Bybit"""
        try:
            df = pd.DataFrame(klines, columns=[
                "startTime", "openPrice", "highPrice", "lowPrice", "closePrice", 
                "volume", "turnover"
            ])
            
            df["startTime"] = pd.to_datetime(df["startTime"].astype('int64'), unit='ms', errors='coerce')
            df.set_index("startTime", inplace=True)
            
            numeric_cols = ["openPrice", "highPrice", "lowPrice", "closePrice", "volume"]
            for col in numeric_cols:
                df[col] = pd.to_numeric(df[col], errors='coerce')
            
            df.columns = ["Open", "High", "Low", "Close", "Volume", "turnover"]
            return df[["Open", "High", "Low", "Close", "Volume"]].sort_index().dropna()
        except Exception as e:
            print(f"Ошибка парсинга Bybit: {e}")
            return None
    
    @staticmethod
    def _parse_okx_data(klines):
        """Парсинг данных OKX"""
        try:
            df = pd.DataFrame(klines, columns=[
                "Open time", "Open", "High", "Low", "Close", "Volume",
                "volCcy", "volCcyQuote", "confirm"
            ])
            
            df["Open time"] = pd.to_datetime(df["Open time"].astype(int), unit="ms", errors='coerce')
            df.set_index("Open time", inplace=True)
            
            numeric_cols = ["Open", "High", "Low", "Close", "Volume"]
            for col in numeric_cols:
                df[col] = pd.to_numeric(df[col], errors='coerce')
            
            return df[["Open", "High", "Low", "Close", "Volume"]].sort_index().dropna()
        except Exception as e:
            print(f"Ошибка парсинга OKX: {e}")
            return None
    
    @staticmethod
    def _parse_bitget_data(klines):
        """Парсинг данных Bitget (spot 8 колонок, mix 7)."""
        try:
            if not klines:
                return None
            n = len(klines[0])
            names = ["startTime", "openPrice", "highPrice", "lowPrice", "closePrice", "volume"]
            if n >= 7:
                names.append("turnover")
            if n >= 8:
                names.append("Trading")
            df = pd.DataFrame([row[:len(names)] for row in klines], columns=names)
            df["startTime"] = pd.to_datetime(df["startTime"].astype('int64'), unit='ms', errors='coerce')
            df.set_index("startTime", inplace=True)
            df = df.rename(columns={
                "openPrice": "Open",
                "highPrice": "High",
                "lowPrice": "Low",
                "closePrice": "Close",
                "volume": "Volume",
            })
            for col in ("Open", "High", "Low", "Close", "Volume"):
                df[col] = pd.to_numeric(df[col], errors='coerce')
            return df[["Open", "High", "Low", "Close", "Volume"]].sort_index().dropna()
        except Exception as e:
            print(f"Ошибка парсинга Bitget: {e}")
            return None

def _parse_price_line(price_line):
    if price_line is None or price_line == '':
        return None
    try:
        price = float(price_line)
    except (TypeError, ValueError):
        return None
    if price <= 0:
        return None
    return price


def _far_enough(left, right) -> bool:
    if left is None or right is None:
        return True
    return abs(left - right) / max(abs(left), 1e-12) > 1e-6


def _ylim_with_wall(df, price_line=None, bounce_line=None, entry_line=None):
    low = float(df['Low'].min())
    high = float(df['High'].max())
    for raw in (price_line, bounce_line, entry_line):
        price = _parse_price_line(raw)
        if price is not None:
            low = min(low, price)
            high = max(high, price)
    span = high - low
    if span <= 0:
        padding = abs(high) * 0.001 or 1e-8
    else:
        padding = span * 0.12
    return low - padding, high + padding


def _candle_x_at(df, unix_ts):
    """Индекс 5m-свечи, которая содержит unix-время обнаружения."""
    try:
        ts = float(unix_ts)
    except (TypeError, ValueError):
        return None
    if ts <= 0:
        return None
    if ts > 1e12:
        ts /= 1000.0
    if ts < 1e9:
        return None
    found = pd.Timestamp(ts, unit='s')
    idx = df.index
    tz = getattr(idx, 'tz', None)
    if tz is not None:
        if found.tzinfo is None:
            found = found.tz_localize('UTC')
        found = found.tz_convert(tz)
    elif found.tzinfo is not None:
        found = found.tz_convert('UTC').tz_localize(None)
    pos = int(idx.searchsorted(found, side='right')) - 1
    if pos < 0 or pos >= len(idx):
        return None
    return pos


def _chart_png_data_url(buffer):
    buffer.seek(0)
    encoded = base64.b64encode(buffer.read()).decode('utf-8')
    return f"data:image/png;base64,{encoded}"


class ChartGenerator:
    """Класс для генерации графиков свечей"""

    @staticmethod
    def generate_mini_chart_base64(df, price_line=None):
        """Мини-график: цена закрытия и линия плотности в том же масштабе."""
        if df is None or df.empty:
            return None

        try:
            closes = df['Close'].astype(float)
            ymin, ymax = _ylim_with_wall(df, price_line)
            xs = range(len(closes))
            wall = _parse_price_line(price_line)

            bg = '#0b0e11'
            with _chart_lock:
                fig, ax = plt.subplots(figsize=(2.6, 1.05), dpi=80)
                ax.plot(xs, closes, color='#7c9cff', linewidth=1.4, alpha=0.95)
                fill_base = float(df['Low'].min())
                ax.fill_between(xs, closes, fill_base, color='#7c9cff', alpha=0.16)
                if wall is not None:
                    ax.axhline(y=wall, color='#f6465d', linestyle='--', linewidth=1.2, alpha=0.9)
                ax.set_xlim(-0.5, len(closes) - 0.5)
                ax.set_ylim(ymin, ymax)
                ax.set_xticks([])
                ax.set_yticks([])
                for spine in ax.spines.values():
                    spine.set_visible(False)
                ax.set_facecolor(bg)
                fig.patch.set_facecolor(bg)
                buffer = io.BytesIO()
                fig.savefig(buffer, format='png', bbox_inches='tight',
                            pad_inches=0.04, facecolor=bg, edgecolor='none')
                plt.close(fig)
            return _chart_png_data_url(buffer)
        except Exception as e:
            print(f"Ошибка генерации мини-графика: {e}")
            return None

    @staticmethod
    def generate_full_chart_bytes(
        df, title="", price_line=None, bounce_line=None, bounce_pct=None,
        found_at=None, entry_line=None,
    ):
        """Полный свечной график с линией плотности и опционально отск.%, PNG-байты."""
        if df is None or df.empty:
            return None

        try:
            plot_df = df[['Open', 'High', 'Low', 'Close', 'Volume']].astype(float).copy()
            wall = _parse_price_line(price_line)
            bounce = _parse_price_line(bounce_line)
            entry = _parse_price_line(entry_line)
            current_price = float(plot_df['Close'].iloc[-1])
            bg = '#0b0e11'
            label = '#f5f7fa'
            style = mpf.make_mpf_style(
                base_mpf_style='nightclouds',
                marketcolors=mpf.make_marketcolors(
                    up='#0ecb81', down='#f6465d',
                    wick={'up': '#0ecb81', 'down': '#f6465d'},
                    edge={'up': '#0ecb81', 'down': '#f6465d'},
                    volume={'up': '#0ecb81', 'down': '#f6465d'},
                ),
                facecolor=bg,
                figcolor=bg,
                edgecolor='#3d4654',
                gridcolor='#2a3140',
                gridstyle='--',
                y_on_right=True,
                rc={
                    'axes.labelcolor': label,
                    'xtick.color': label,
                    'ytick.color': label,
                    'xtick.labelsize': 12,
                    'ytick.labelsize': 12,
                    'text.color': label,
                    'axes.titlecolor': label,
                    'font.weight': 'bold',
                },
            )
            hline_prices = []
            hline_colors = []
            hline_styles = []
            if wall is not None:
                line_color = '#28a745' if wall <= current_price else '#dc3545'
                hline_prices.append(wall)
                hline_colors.append(line_color)
                hline_styles.append('--')
            if bounce is not None and _far_enough(bounce, wall):
                hline_prices.append(bounce)
                hline_colors.append('#d4a017')
                hline_styles.append(':')
            if entry is not None and _far_enough(entry, wall) and _far_enough(entry, bounce):
                hline_prices.append(entry)
                hline_colors.append('#20b2aa')
                hline_styles.append('-')

            plot_kwargs = dict(
                type='candle',
                style=style,
                title='',
                ylabel='',
                ylabel_lower='',
                volume=True,
                figratio=(2, 1),
                figscale=1.15,
                panel_ratios=(5, 1),
                tight_layout=True,
                returnfig=True,
                show_nontrading=False,
                scale_padding={'left': 0.08, 'top': 0.06, 'right': 0.32, 'bottom': 0.28},
            )
            if hline_prices:
                plot_kwargs['hlines'] = dict(
                    hlines=hline_prices,
                    colors=hline_colors,
                    linestyle=hline_styles,
                    linewidths=1.6,
                )
                ymin, ymax = _ylim_with_wall(plot_df, wall, bounce, entry)
                plot_kwargs['ylim'] = (ymin, ymax)

            with _chart_lock:
                fig, axes = mpf.plot(plot_df, **plot_kwargs)
                fig.suptitle('')
                for ax in fig.axes:
                    ax.set_title('')
                    ax.set_ylabel('')
                    ax.set_xlabel('')
                    if getattr(ax, 'yaxis', None) is not None:
                        ax.yaxis.label.set_visible(False)
                    if getattr(ax, 'xaxis', None) is not None:
                        ax.xaxis.label.set_visible(False)
                    ax.yaxis.tick_right()
                    ax.yaxis.set_label_position('right')
                    ax.tick_params(
                        axis='y', colors=label, labelcolor=label, labelsize=12,
                        width=1.15, length=4.5, which='both',
                        left=False, labelleft=False, right=True, labelright=True,
                    )
                    ax.tick_params(
                        axis='x', colors=label, labelcolor=label, labelsize=12,
                        width=1.15, length=4.5, which='both',
                    )
                    ax.yaxis.get_offset_text().set_color(label)
                    ax.xaxis.get_offset_text().set_color(label)
                    for tick in list(ax.get_xticklabels()) + list(ax.get_yticklabels()):
                        tick.set_color(label)
                        tick.set_fontsize(12)
                        tick.set_fontweight('bold')
                found_x = _candle_x_at(plot_df, found_at)
                if found_x is not None:
                    axes[0].axvline(
                        x=found_x, color='#9db4ff', linestyle='-.',
                        linewidth=1.4, alpha=0.95, zorder=4,
                    )
                    _, y_top = axes[0].get_ylim()
                    axes[0].annotate(
                        'найдена',
                        xy=(found_x, y_top),
                        xytext=(5, -10),
                        textcoords='offset points',
                        fontsize=11, color='#c5d4ff', fontweight='bold',
                        va='top', ha='left',
                    )
                if entry is not None and _far_enough(entry, wall) and _far_enough(entry, bounce):
                    axes[0].annotate(
                        'вход фьюч',
                        xy=(len(plot_df) - 1, entry),
                        xytext=(-8, 6),
                        textcoords='offset points',
                        fontsize=11, color='#5eead4', fontweight='bold',
                        va='bottom', ha='right',
                    )
                buffer = io.BytesIO()
                fig.savefig(buffer, format='png', dpi=110, bbox_inches='tight',
                            pad_inches=0.06, facecolor=bg, edgecolor='none')
                plt.close(fig)
            return buffer.getvalue()
        except Exception as e:
            print(f"Ошибка генерации полного графика: {e}")
            return None

    @staticmethod
    def generate_full_chart_base64(
        df, title="", price_line=None, bounce_line=None, bounce_pct=None,
        found_at=None, entry_line=None,
    ):
        """Полный свечной график с линией плотности."""
        raw = ChartGenerator.generate_full_chart_bytes(
            df, title, price_line, bounce_line=bounce_line, bounce_pct=bounce_pct,
            found_at=found_at, entry_line=entry_line,
        )
        if not raw:
            return None
        encoded = base64.b64encode(raw).decode('utf-8')
        return f"data:image/png;base64,{encoded}"

# ====================== API ЭНДПОИНТЫ ======================

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/densities')
def get_densities():
    """API для получения всех плотностей"""
    try:
        data = scanner_manager.get_all_densities()
        return jsonify(data)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/status')
def get_status():
    """API для получения статуса системы"""
    status = {
        'is_running': scanner_manager.is_running,
        'exchanges': {},
        'total_densities': 0,
        'last_updates': {}
    }
    
    total = 0
    for exchange, data in scanner_manager.all_densities.items():
        bids_count = len(data['bids'])
        asks_count = len(data['asks'])
        total += bids_count + asks_count
        
        status['exchanges'][exchange] = {
            'bids': bids_count,
            'asks': asks_count,
            'last_update': data['last_update']
        }
        status['last_updates'][exchange] = scanner_manager.last_update_time.get(exchange, 0)
    
    status['total_densities'] = total
    status['outcomes'] = read_journal(limit=0)
    return jsonify(status)

@app.route('/api/outcomes')
def get_outcomes():
    """Журнал закрытых плотностей: отскок / снятие / пробой"""
    try:
        want_all = request.args.get('all', '').lower() in ('1', 'true', 'yes')
        return jsonify(read_journal(limit=None if want_all else 40))
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/start')
def start_scanner():
    """API для запуска сканера"""
    if not scanner_manager.is_running:
        scanner_manager.start_background_scanner()
        return jsonify({'status': 'started', 'message': 'Сканер запущен'})
    return jsonify({'status': 'already_running', 'message': 'Сканер уже запущен'})

@app.route('/api/stop')
def stop_scanner():
    """API для остановки сканера"""
    scanner_manager.stop()
    return jsonify({'status': 'stopped', 'message': 'Сканер остановлен'})

@app.route('/api/health')
def health_check():
    """Health check endpoint"""
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.now().isoformat(),
        'scanner_running': scanner_manager.is_running
    })

@app.route('/api/charts/<chart_id>.png')
def get_outcome_chart(chart_id):
    path = chart_path(chart_id)
    if path is None or not path.is_file():
        abort(404)
    return send_file(path, mimetype='image/png')

@app.route('/api/chart/mini/<exchange>/<path:symbol>')
def get_mini_chart(exchange, symbol):
    """API для получения мини-графика в base64"""
    try:
        price_line = request.args.get('price_line', type=float)
        df = asyncio.run(
            UniversalChartGenerator.get_candles_safe(exchange, symbol, "5m", 100)
        )

        if df is not None and not df.empty:
            chart_base64 = ChartGenerator.generate_mini_chart_base64(df, price_line)
            if chart_base64:
                return jsonify({
                    'success': True,
                    'chart': chart_base64,
                    'current_price': float(df['Close'].iloc[-1]) if 'Close' in df.columns and len(df) > 0 else 0,
                    'price_change': float(df['Close'].iloc[-1] - df['Open'].iloc[-1]) if len(df) > 0 else 0,
                    'price_change_percent': float((df['Close'].iloc[-1] - df['Open'].iloc[-1]) / df['Open'].iloc[-1] * 100) if len(df) > 0 and df['Open'].iloc[-1] != 0 else 0,
                    'candle_count': len(df)
                })

        return jsonify({'success': False, 'error': 'No data available'})

    except Exception as e:
        print(f"Error in get_mini_chart for {exchange}/{symbol}: {e}")
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/chart/full/<exchange>/<path:symbol>')
def get_full_chart(exchange, symbol):
    """Полный график: 1m и 5m в base64."""
    try:
        async def _load():
            return await asyncio.gather(
                UniversalChartGenerator.get_candles_safe(exchange, symbol, '1m', 100),
                UniversalChartGenerator.get_candles_safe(exchange, symbol, '5m', 100),
            )

        df_1m, df_5m = asyncio.run(_load())
        price_line = request.args.get('price_line', type=float)

        def _png(df, interval):
            if df is None or getattr(df, 'empty', True):
                return None
            return ChartGenerator.generate_full_chart_base64(
                df,
                title=f"{exchange} - {symbol} {interval}",
                price_line=price_line,
            )

        chart_1m = _png(df_1m, '1m')
        chart_5m = _png(df_5m, '5m')
        if not chart_1m and not chart_5m:
            return jsonify({'success': False, 'error': 'No data available'})

        df = df_5m if df_5m is not None and not getattr(df_5m, 'empty', True) else df_1m
        return jsonify({
            'success': True,
            'chart': chart_5m or chart_1m,
            'chart_1m': chart_1m,
            'chart_5m': chart_5m,
            'candle_count': len(df) if df is not None else 0,
            'start_date': df.index[0].strftime('%Y-%m-%d %H:%M') if df is not None and len(df) > 0 else 'N/A',
            'end_date': df.index[-1].strftime('%Y-%m-%d %H:%M') if df is not None and len(df) > 0 else 'N/A',
            'price_range': f"{df['Low'].min():.6f} - {df['High'].max():.6f}" if df is not None and len(df) > 0 else 'N/A',
            'current_price': float(df['Close'].iloc[-1]) if df is not None and len(df) > 0 else 0,
            'avg_volume': float(df['Volume'].mean()) if df is not None and len(df) > 0 else 0,
        })

    except Exception as e:
        print(f"Error in get_full_chart for {exchange}/{symbol}: {e}")
        return jsonify({'success': False, 'error': str(e)})

# Обработка завершения работы
def shutdown_handler(signum, frame):
    print("\n📢 Получен сигнал завершения работы...")
    scanner_manager.stop()
    print("✅ Приложение завершает работу")
    sys.exit(0)

if __name__ == '__main__':
    # Регистрируем обработчики сигналов
    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)
    
    # Запускаем сканер при старте приложения
    print("🚀 Запуск Flask приложения...")
    scanner_manager.start_background_scanner()
    
    # Локально: порт 5000. На сервере: PORT=80
    port = int(os.environ.get('PORT', '5000'))
    debug = os.environ.get('FLASK_DEBUG', '0') == '1'
    app.run(debug=debug, host='0.0.0.0', port=port, use_reloader=False)