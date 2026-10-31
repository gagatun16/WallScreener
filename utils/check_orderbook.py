''' Проверка на заполненность стакана относительно плотности '''

from config.params import MIN_VOLATILITY

# Для того чтобы получить мин. TP минус 0.2 
MIN_VOLATILITY = MIN_VOLATILITY - 0.2

def check_orderbook(array_bids, array_asks, price, qty, percent, long) -> bool:
    sum_qty = 0
    new_arr_for_bids_qty = []
    new_arr_for_asks_qty = []
    if percent > MIN_VOLATILITY:
        if long:
            for bid in array_bids:
                bid_price = float(bid[0]) 
                bid_qty = float(bid[1]) 
                percent_to_plotnost = abs(price * 100 / bid_price - 100)
                if bid_price > price and percent_to_plotnost <= MIN_VOLATILITY:
                    sum_qty += bid_qty
            if sum_qty < qty:
                flag = False
                return flag
            else:
                flag = True
                return flag
        else:
            # Собираю нужные плотности в одном месте
            for ask in array_asks:
                ask_price = float(ask[0])
                ask_qty = float(ask[1])
                percent_to_plotnost = abs(price * 100 / ask_price - 100)
                if ask_price < price and percent_to_plotnost <= MIN_VOLATILITY:
                    sum_qty += ask_qty
            # Суммирую плотности
            if sum_qty < qty:
                flag = False
                return flag
            else:
                flag = True
                return flag
            
    else:
        current_ask_price = float(array_asks[0][0])
        current_bid_price = float(array_bids[0][0])
        if long:
            # Собираю нужные плотности в одном месте
            percent_to_bid_plotnost = abs(price * 100 / current_bid_price - 100)
            percent_to_high = MIN_VOLATILITY - percent_to_bid_plotnost
            for bid in array_bids:
                bid_price = float(bid[0])
                bid_qty = float(bid[1])

                if bid_price > price:
                    new_arr_for_bids_qty.append(bid_qty)
            
            for ask in array_asks:
                ask_price = float(ask[0])
                ask_qty = float(ask[1])
                percent_to_ask_plotnost = abs(price * 100 / ask_price - 100)
                if percent_to_ask_plotnost < percent_to_high:
                    new_arr_for_asks_qty.append(ask_qty)

            if (sum(new_arr_for_asks_qty) + sum(new_arr_for_bids_qty)) < qty:
                flag = False
                return flag
            else:
                flag = True
                return flag
        
        else:
            percent_to_ask_plotnost = abs(price * 100 / current_ask_price - 100)
            percent_to_low = MIN_VOLATILITY - percent_to_ask_plotnost
            for ask in array_asks:
                ask_price = float(ask[0])
                ask_qty = float(ask[1])

                if ask_price < price:
                    new_arr_for_asks_qty.append(ask_qty)

            for bid in array_bids:
                bid_price = float(bid[0])
                bid_qty = float(bid[1])
                percent_to_bid_plotnost = abs(price * 100 / bid_price - 100)

                if percent_to_bid_plotnost < percent_to_low:
                    new_arr_for_bids_qty.append(bid_qty)

            if (sum(new_arr_for_asks_qty) + sum(new_arr_for_bids_qty)) < qty:
                flag = False
                return flag
            else:
                flag = True
                return flag