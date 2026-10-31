from scanner_manager import ExchangeScanner

from exchanges.binance.symbols import get_symbols as binance_get_symbols
from exchanges.binance.orderbook import analyze_order_books_optimized as binance_analyze_orderbooks
from exchanges.binance.density import update_plotnost_time as binance_update_densities

from exchanges.bingx.symbols import get_symbols as bingx_get_symbols
from exchanges.bingx.orderbook import analyze_order_books as bingx_analyze_orderbooks
from exchanges.bingx.density import update_plotnost_time as bingx_update_densities

from exchanges.bitget.symbols import get_symbols as bitget_get_symbols
from exchanges.bitget.orderbook import analyze_order_books as bitget_analyze_orderbooks
from exchanges.bitget.density import update_plotnost_time as bitget_update_densities

from exchanges.bybit.symbols import get_symbols as bybit_get_symbols
from exchanges.bybit.orderbook import analyze_order_books as bybit_analyze_orderbooks
from exchanges.bybit.density import update_plotnost_time as bybit_update_densities

from exchanges.mexc.symbols import get_symbols as mexc_get_symbols
from exchanges.mexc.orderbook import analyze_order_books_optimized as mexc_analyze_orderbooks
from exchanges.mexc.density import update_plotnost_time as mexc_update_densities

from exchanges.okx.symbols import get_symbols as okx_get_symbols
from exchanges.okx.orderbook import analyze_order_books_optimized as okx_analyze_orderbooks
from exchanges.okx.density import update_plotnost_time as okx_update_densities

SCANNERS = [
    ExchangeScanner("Binance", 
                   binance_get_symbols, 
                   binance_analyze_orderbooks, 
                   binance_update_densities),
    
    ExchangeScanner("BingX", 
                   bingx_get_symbols, 
                   bingx_analyze_orderbooks, 
                   bingx_update_densities),
    
    ExchangeScanner("Bitget", 
                   bitget_get_symbols, 
                   bitget_analyze_orderbooks, 
                   bitget_update_densities),
    
    ExchangeScanner("Bybit", 
                   bybit_get_symbols, 
                   bybit_analyze_orderbooks, 
                   bybit_update_densities),
    
    ExchangeScanner("MEXC", 
                   mexc_get_symbols, 
                   mexc_analyze_orderbooks, 
                   mexc_update_densities),
    
    ExchangeScanner("OKX", 
                   okx_get_symbols, 
                   okx_analyze_orderbooks, 
                   okx_update_densities),
]