from binance.client import Client
from binance.websockets import BinanceSocketManager
import sys
import time
from binance.enums import *
from binance import exceptions
import logging
from datetime import datetime, date
import requests

order_logger = logging.getLogger('order_tracker')
order_logger.setLevel(logging.DEBUG)
exception_logger = logging.getLogger('exception_tracker')
exception_logger.setLevel(logging.DEBUG)

order_log_file_handler = logging.FileHandler('logs\order_tracker_%s.log' % datetime.utcnow().date().isoformat())
order_log_file_handler.setLevel(logging.INFO)
exception_log_file_handler = logging.FileHandler('logs\exception_tracker_%s.log' % datetime.utcnow().date().isoformat())
exception_log_file_handler.setLevel(logging.INFO)

order_logger.addHandler(order_log_file_handler)
exception_logger.addHandler(exception_log_file_handler)

api_key = 'RO0yfaBhlsb6rRZ3eAajt9Ptx347izGlfihXOskGhnk1NFcMVn4en7uTdtHAFfgD'
api_secret = 'suH3HixQOlGKCeV4vqA8eEhU1lFJgJQuexzZIomkJJ6JUwOnWk8ugLDdVq2XJBU7'
client = Client(api_key, api_secret)
bm = BinanceSocketManager(client)

bnbbtc_conn_key = None
ethbtc_conn_key = None
bnbeth_conn_key = None
neobtc_conn_key = None
neoeth_conn_key = None
btcusdt_conn_key = None
ethusdt_conn_key = None
bnbusdt_conn_key = None
account_conn_key = None

COIN1 = 'BNB'
COIN2 = 'ETH'
COIN3 = 'BTC'

EMPTY_ORDER = {'status': 'None',
               'orderId': 'None',
               'price': 0.0,
               'origQty': 0.0,
               'executedQty': 0.0,
               'side': 'None'}

# pair priorities USDT always comes last in a pair.  BTC always comes after all
# coins other than USDT.  ETH comes after all coins other than USDT and BTC.
# also, pair should go COIN1/COIN3, COIN2/COIN1, COIN2/COIN3
PAIR1 = 'BNBBTC'
PAIR2 = 'ETHBTC'
PAIR3 = 'BNBETH'

COIN1 = 'BTC'
COIN2 = 'ETH'
COIN3 = 'USDT'
PAIR1 = 'BTCUSDT'
PAIR2 = 'ETHBTC'
PAIR3 = 'ETHUSDT'

TICK = {'BNBBTC': 0.0000001,
        'ETHBTC': 0.000001,
        'BNBETH': 0.000001,
        'NEOBTC': 0.000001,
        'NEOETH': 0.000001,
        'BTCUSDT': 0.01,
        'ETHUSDT': 0.01}
PRICE_PRECISION = {'BNBBTC': 7,
                   'ETHBTC': 6,
                   'BNBETH': 6,
                   'NEOBTC': 6,
                   'NEOETH': 6,
                   'BTCUSDT': 2,
                   'ETHUSDT': 2}
PRICE_FORMAT = {'BNBBTC': '%.7f',
                'ETHBTC': '%.6f',
                'BNBETH': '%.6f',
                'NEOBTC': '%.6f',
                'NEOETH': '%.6f',
                'BTCUSDT': '%.2f',
                'ETHUSDT': '%.2f'}
QUANTITY_PRECISION = {'BNBBTC': 2,
                      'ETHBTC': 3,
                      'BNBETH': 2,
                      'NEOBTC': 2,
                      'NEOETH': 2,
                      'BTCUSDT': 6,
                      'ETHUSDT': 5}
SPREAD_THRESHOLD = {'BNBBTC': 0.4,
                    'ETHBTC': 0.75,
                    'BNBETH': 0.4,
                    'NEOBTC': 0.5,
                    'NEOETH': 0.5,
                    'BTCUSDT': 0.75,
                    'ETHUSDT': 0.5}
MIN_AMOUNT = {'BNBBTC': 1.0,
              'ETHBTC': 0.001,
              'BNBETH': 1.0,
              'NEOBTC': 0.01,
              'NEOETH': 0.01,
              'BTCUSDT': 0.000001,
              'ETHUSDT': 0.00001}
MIN_NOTIONAL = {'BNBBTC': 0.0,
                'ETHBTC': 0.001,
                'BNBETH': 0.0,
                'NEOBTC': 0.0,
                'NEOETH': 0.0,
                'BTCUSDT': 1.0,
                'ETHUSDT': 20.0}

FEE = 0.0005
THRESHOLD = 1.0019 # + (4 * FEE)
BNB_QUANTITY = 6.0


class OrderBook:
    bid = 0
    ask = sys.maxsize


raw_order_book = {'BNBBTC': OrderBook(),
                  'ETHBTC': OrderBook(),
                  'BNBETH': OrderBook(),
                  'NEOBTC': OrderBook(),
                  'NEOETH': OrderBook(),
                  'BTCUSDT': OrderBook(),
                  'ETHUSDT': OrderBook(),
                  'BNBUSDT': OrderBook()}

balance_book = {'BNB': 0.0,
                'ETH': 0.0,
                'BTC': 0.0,
                'NEO': 0.0,
                'USDT': 0.0}

trade_order_book = {}


def process_bnbbtc_depth_message(msg):
    global raw_order_book
    raw_order_book['BNBBTC'].bid = float(msg['bids'][0][0])
    raw_order_book['BNBBTC'].ask = float(msg['asks'][0][0])


def process_ethbtc_depth_message(msg):
    global raw_order_book
    raw_order_book['ETHBTC'].bid = float(msg['bids'][0][0])
    raw_order_book['ETHBTC'].ask = float(msg['asks'][0][0])


def process_bnbeth_depth_message(msg):
    global raw_order_book
    raw_order_book['BNBETH'].bid = float(msg['bids'][0][0])
    raw_order_book['BNBETH'].ask = float(msg['asks'][0][0])


def process_neobtc_depth_message(msg):
    global raw_order_book
    raw_order_book['NEOBTC'].bid = float(msg['bids'][0][0])
    raw_order_book['NEOBTC'].ask = float(msg['asks'][0][0])


def process_neoeth_depth_message(msg):
    global raw_order_book
    raw_order_book['NEOETH'].bid = float(msg['bids'][0][0])
    raw_order_book['NEOETH'].ask = float(msg['asks'][0][0])


def process_btcusdt_depth_message(msg):
    global raw_order_book
    raw_order_book['BTCUSDT'].bid = float(msg['bids'][0][0])
    raw_order_book['BTCUSDT'].ask = float(msg['asks'][0][0])


def process_ethusdt_depth_message(msg):
    global raw_order_book
    raw_order_book['ETHUSDT'].bid = float(msg['bids'][0][0])
    raw_order_book['ETHUSDT'].ask = float(msg['asks'][0][0])


def process_bnbusdt_depth_message(msg):
    global raw_order_book
    raw_order_book['BNBUSDT'].bid = float(msg['bids'][0][0])
    raw_order_book['BNBUSDT'].ask = float(msg['asks'][0][0])


def process_account_message(msg):
    global balance_book
    #print('user stream message: ', msg)
    if 'e' in msg and msg['e'] != 'outboundAccountInfo':
        # we only care about account info for now
        return
    if 'B' not in msg:
        # the outboundAccountInfo message should have balances
        return
    for asset in msg['B']:
        if asset['a'] in balance_book:
            #print('asset: ', asset['a'], 'balance: ', asset['f'])
            balance_book[asset['a']] = float(asset['f'])


def update_order(order, original_quantity, check_level):
    global raw_order_book
    global client

    if order['status'] == 'FILLED':
        return None

    new_order = None

    order_quantity = float(order['origQty'])
    executed_quantity = float(order['executedQty'])
    if (order_quantity - executed_quantity) < 10*MIN_AMOUNT[order['symbol']]:
        # May hit MIN NOTIONAL error if we try to re-submit the order for the
        # remaining amount.  Instead, let it ride, we shouldn't lose too much.
        return order
    new_quantity = round(order_quantity - executed_quantity,
                         QUANTITY_PRECISION[order['symbol']])

    # don't re-place the order if the price isn't going to change.  we will just
    # lose our place in line in the order book and damage our fills per order+cancels
    # metrics.
    if order['side'] == 'BUY':
        if raw_order_book[order['symbol']].bid == float(order['price']):
            return order
        price = raw_order_book[order['symbol']].bid
        if check_level >= 2:
            price = round(price + TICK[order['symbol']], PRICE_PRECISION[order['symbol']])
    elif order['side'] == 'SELL':
        if raw_order_book[order['symbol']].ask == float(order['price']):
            return order
        price = raw_order_book[order['symbol']].ask
        if check_level >= 2:
            price = round(price - TICK[order['symbol']], PRICE_PRECISION[order['symbol']])

    if price * new_quantity < MIN_NOTIONAL[order['symbol']]:
        print('Value under min notional')
        print('Price: ', price, 'quantity: ', new_quantity, 'min: ', MIN_NOTIONAL[order['symbol']])
        return order

    try:
        client.cancel_order(symbol=order['symbol'], orderId=order['orderId'])
    except exceptions.BinanceAPIException as e:
        if e.message == 'UNKNOWN_ORDER' or e.code == -2011:
            print('Order already filled')
            return
        else:
            exception_logger.info('Time: ' + datetime.utcnow().isoformat())
            exception_logger.info(e)
            raise e
    if order['side'] == 'BUY':
        print('updating bid to: ', price, new_quantity)
        new_order = client.order_limit_buy(symbol=order['symbol'],
                                           price=str(price),
                                           quantity=new_quantity)
    elif order['side'] == 'SELL':
        print('updating ask to: ', price, new_quantity)
        new_order = client.order_limit_sell(symbol=order['symbol'],
                                            price=str(price),
                                            quantity=new_quantity)
    return new_order


def cancel_order(order):
    new_quantity = round(float(order['origQty']) - float(order['executedQty']), 2)
    print('Canceled: ', order['symbol'])
    try:
        client.cancel_order(symbol=order['symbol'], orderId=order['orderId'])
    except exceptions.BinanceAPIException as e:
        if e.message == 'UNKNOWN_ORDER' or e.code == -2011:
            print('Order already filled')
            return
        else:
            exception_logger.error('Time: ' + datetime.utcnow().isoformat())
            exception_logger.error(e)
            raise e
    # try:
    #     if order['side'] == 'BUY':
    #         client.order_market_buy(symbol=order['symbol'], quantity=new_quantity)
    #     else:
    #         client.order_market_sell(symbol=order['symbol'], quantity=new_quantity)
    # except Exception as e:
    #     print('Exception while attempting to cancel order: ', order)
    #     print(e)


def quick_calc(a_quantity, b_per_a, c_per_b, a_per_c):
    b_quantity = b_per_a * a_quantity
    c_quantity = c_per_b * b_quantity
    a_result = a_per_c * c_quantity

    return b_quantity, c_quantity, a_result


def build_trade_order_book():
    global raw_order_book
    pair1_spread = round((raw_order_book[PAIR1].ask - raw_order_book[PAIR1].bid) / TICK[PAIR1], 0)
    pair2_spread = round((raw_order_book[PAIR2].ask - raw_order_book[PAIR2].bid) / TICK[PAIR2], 0)
    pair3_spread = round((raw_order_book[PAIR3].ask - raw_order_book[PAIR3].bid) / TICK[PAIR3], 0)
    print(PAIR1 + ' spread: ', pair1_spread)
    print(PAIR2 + ' spread: ', pair2_spread)
    print(PAIR3 + ' spread: ', pair3_spread)

    # pick a price in the middle of the spread and see if that works for arbitrage
    trade_order = {PAIR1: OrderBook(),
                   PAIR2: OrderBook(),
                   PAIR3: OrderBook()}
    trade_order[PAIR1].bid = max(raw_order_book[PAIR1].bid,
                                 raw_order_book[PAIR1].ask - (
                                         SPREAD_THRESHOLD[PAIR1] * pair1_spread * TICK[PAIR1]))
    trade_order[PAIR1].ask = min(raw_order_book[PAIR1].ask,
                                 raw_order_book[PAIR1].bid + (
                                         SPREAD_THRESHOLD[PAIR1] * pair1_spread * TICK[PAIR1]))
    trade_order[PAIR2].bid = max(raw_order_book[PAIR2].bid,
                                 raw_order_book[PAIR2].ask - (
                                         SPREAD_THRESHOLD[PAIR2] * pair2_spread * TICK[PAIR2]))
    trade_order[PAIR2].ask = min(raw_order_book[PAIR2].ask,
                                 raw_order_book[PAIR2].bid + (
                                         SPREAD_THRESHOLD[PAIR2] * pair2_spread * TICK[PAIR2]))
    trade_order[PAIR3].bid = max(raw_order_book[PAIR3].bid,
                                 raw_order_book[PAIR3].ask - (
                                         SPREAD_THRESHOLD[PAIR3] * pair3_spread * TICK[PAIR3]))
    trade_order[PAIR3].ask = min(raw_order_book[PAIR3].ask,
                                 raw_order_book[PAIR3].bid + (
                                         SPREAD_THRESHOLD[PAIR3] * pair3_spread * TICK[PAIR3]))

    trade_order[PAIR1].bid = round(trade_order[PAIR1].bid, PRICE_PRECISION[PAIR1])
    trade_order[PAIR1].ask = round(trade_order[PAIR1].ask, PRICE_PRECISION[PAIR1])
    trade_order[PAIR2].bid = round(trade_order[PAIR2].bid, PRICE_PRECISION[PAIR2])
    trade_order[PAIR2].ask = round(trade_order[PAIR2].ask, PRICE_PRECISION[PAIR2])
    trade_order[PAIR3].bid = round(trade_order[PAIR3].bid, PRICE_PRECISION[PAIR3])
    trade_order[PAIR3].ask = round(trade_order[PAIR3].ask, PRICE_PRECISION[PAIR3])

    print(PAIR1 + ' bid: ', PRICE_FORMAT[PAIR1] % trade_order[PAIR1].bid,
          PAIR1 + ' ask: ', PRICE_FORMAT[PAIR1] % trade_order[PAIR1].ask)
    print(PAIR2 + ' bid: ', PRICE_FORMAT[PAIR2] % trade_order[PAIR2].bid,
          PAIR2 + ' ask: ', PRICE_FORMAT[PAIR2] % trade_order[PAIR2].ask)
    print(PAIR3 + ' bid: ', PRICE_FORMAT[PAIR3] % trade_order[PAIR3].bid,
          PAIR3 + ' ask: ', PRICE_FORMAT[PAIR3] % trade_order[PAIR3].ask)

    return trade_order


def calculate_coin_ratio(coin1, coin2):
    global trade_order_book
    if coin1+coin2 in [PAIR1, PAIR2, PAIR3]:
        # print(coin1, coin2, 'bid: ', trade_order_book[coin1+coin2].bid, 'ask: ', trade_order_book[coin1+coin2].ask)
        coin1_per_coin2 = 1 / trade_order_book[coin1+coin2].bid
        coin2_per_coin1 = trade_order_book[coin1+coin2].ask
    elif coin2+coin1 in [PAIR1, PAIR2, PAIR3]:
        # print(coin1, coin2, 'bid: ', trade_order_book[coin2+coin1].bid, 'ask: ', trade_order_book[coin2+coin1].ask)
        coin2_per_coin1 = 1 / trade_order_book[coin2+coin1].bid
        coin1_per_coin2 = trade_order_book[coin2+coin1].ask
    else:
        error_string = 'No pairs found for coins', coin1, coin2, 'in: ', PAIR1, PAIR2, PAIR3
        print(error_string)
        raise Exception(error_string)

    return coin1_per_coin2, coin2_per_coin1


def convert_coins(coin1, coin2, quantity):
    global trade_order_book
    # print('convert ' + coin1 + ' to ' + coin2 + ' quantity: ', quantity)
    if coin1+coin2 in [PAIR1, PAIR2, PAIR3]:
        # sell
        pair = coin1+coin2
        price = PRICE_FORMAT[pair] % trade_order_book[pair].ask
        adjusted_quantity = round(quantity, QUANTITY_PRECISION[pair])
        # print('SELL: ', pair, 'price: ', price, 'quantity: ', adjusted_quantity)
        if trade_order_book[pair].ask * adjusted_quantity < MIN_NOTIONAL[pair]:
            print('Value under min notional')
            print('Price: ', price, 'quantity: ', adjusted_quantity, 'min: ', MIN_NOTIONAL[pair])
            return None
        order = client.order_limit_sell(symbol=pair,
                                        price=price,
                                        quantity=round(adjusted_quantity, QUANTITY_PRECISION[pair]))
    elif coin2+coin1 in [PAIR1, PAIR2, PAIR3]:
        # buy
        pair = coin2+coin1
        price = PRICE_FORMAT[pair] % trade_order_book[pair].bid
        coin1_per_coin2, coin2_per_coin1 = calculate_coin_ratio(coin1, coin2)
        adjusted_quantity = round(quantity * coin2_per_coin1, QUANTITY_PRECISION[pair])
        # convert coin1 quantity to coin2 quantity
        # print('BUY: ', pair, 'price: ', price, 'quantity: ', adjusted_quantity)
        if trade_order_book[pair].bid * adjusted_quantity < MIN_NOTIONAL[pair]:
            print('Value under min notional')
            print('Price: ', price, 'quantity: ', adjusted_quantity, 'min: ', MIN_NOTIONAL[pair])
            return None
        order = client.order_limit_buy(symbol=pair,
                                       price=price,
                                       quantity=round(adjusted_quantity, QUANTITY_PRECISION[pair]))
    else:
        order = None

    # print('Order: ', order)
    return order


def print_order_status(pair1_order, pair2_order, pair3_order):
    status_string = 'Status:  '
    if pair1_order is None or pair1_order['status'] == 'None':
        status_string += '---  '
    else:
        status_string += '{:3d}  '.format(round(100*float(pair1_order['executedQty'])/float(pair1_order['origQty'])))
    if pair2_order is None or pair2_order['status'] == 'None':
        status_string += '---  '
    else:
        status_string += '{:3d}  '.format(round(100*float(pair2_order['executedQty'])/float(pair2_order['origQty'])))
    if pair3_order is None or pair3_order['status'] == 'None':
        status_string += '---  '
    else:
        status_string += '{:3d}  '.format(round(100*float(pair3_order['executedQty'])/float(pair3_order['origQty'])))

    print(status_string)


def query_initial_coin_balances():
    result = client.get_account()
    for asset in result['balances']:
        if asset['asset'] == COIN1:
            balance_book[asset['asset']] += float(asset['free'])
        elif asset['asset'] == COIN2:
            balance_book[asset['asset']] += float(asset['free'])
        elif asset['asset'] == COIN3:
            balance_book[asset['asset']] += float(asset['free'])
        elif asset['asset'] == 'BNB':
            balance_book[asset['asset']] += float(asset['free'])


def launch_socket_listeners():
    global bm
    global bnbbtc_conn_key
    global ethbtc_conn_key
    global bnbeth_conn_key
    global neobtc_conn_key
    global neoeth_conn_key
    global btcusdt_conn_key
    global ethusdt_conn_key
    global bnbusdt_conn_key
    global account_conn_key
    global raw_order_book

    bm = BinanceSocketManager(client)
    # start any sockets here, i.e a trade socket
    bnbbtc_conn_key = bm.start_depth_socket('BNBBTC',
                                            process_bnbbtc_depth_message,
                                            depth=BinanceSocketManager.WEBSOCKET_DEPTH_5)
    ethbtc_conn_key = bm.start_depth_socket('ETHBTC',
                                            process_ethbtc_depth_message,
                                            depth=BinanceSocketManager.WEBSOCKET_DEPTH_5)
    bnbeth_conn_key = bm.start_depth_socket('BNBETH',
                                            process_bnbeth_depth_message,
                                            depth=BinanceSocketManager.WEBSOCKET_DEPTH_5)
    neobtc_conn_key = bm.start_depth_socket('NEOBTC',
                                            process_neobtc_depth_message,
                                            depth=BinanceSocketManager.WEBSOCKET_DEPTH_5)
    neoeth_conn_key = bm.start_depth_socket('NEOETH',
                                            process_neoeth_depth_message,
                                            depth=BinanceSocketManager.WEBSOCKET_DEPTH_5)
    btcusdt_conn_key = bm.start_depth_socket('BTCUSDT',
                                             process_btcusdt_depth_message,
                                             depth=BinanceSocketManager.WEBSOCKET_DEPTH_5)
    ethusdt_conn_key = bm.start_depth_socket('ETHUSDT',
                                             process_ethusdt_depth_message,
                                             depth=BinanceSocketManager.WEBSOCKET_DEPTH_5)
    bnbusdt_conn_key = bm.start_depth_socket('BNBUSDT',
                                             process_bnbusdt_depth_message,
                                             depth=BinanceSocketManager.WEBSOCKET_DEPTH_5)
    account_conn_key = bm.start_user_socket(process_account_message)
    # then start the socket manager
    bm.start()
    # wait till we have data for all pairs
    print('initialize order book')
    while raw_order_book['BNBETH'].bid == 0 or \
          raw_order_book['ETHBTC'].bid == 0 or \
          raw_order_book['BNBBTC'].bid == 0 or \
          raw_order_book['NEOBTC'].bid == 0 or \
          raw_order_book['NEOETH'].bid == 0 or \
          raw_order_book['BTCUSDT'].bid == 0 or \
          raw_order_book['ETHUSDT'].bid == 0 or \
          raw_order_book['BNBUSDT'].bid == 0:
        time.sleep(1)


def shutdown_socket_listeners():
    global bm
    global bnbbtc_conn_key
    global ethbtc_conn_key
    global bnbeth_conn_key
    global neobtc_conn_key
    global neoeth_conn_key
    global btcusdt_conn_key
    global ethusdt_conn_key
    global bnbusdt_conn_key
    global account_conn_key
    bm.stop_socket(bnbbtc_conn_key)
    bm.stop_socket(ethbtc_conn_key)
    bm.stop_socket(bnbeth_conn_key)
    bm.stop_socket(neobtc_conn_key)
    bm.stop_socket(neoeth_conn_key)
    bm.stop_socket(btcusdt_conn_key)
    bm.stop_socket(ethusdt_conn_key)
    bm.stop_socket(bnbusdt_conn_key)
    bm.stop_socket(account_conn_key)


def cancel_all_orders():
    global client
    orders = client.get_all_orders(PAIR1)
    for order in orders:
        client.cancel_order(order['orderId'])
    orders = client.get_all_orders(PAIR2)
    for order in orders:
        client.cancel_order(order['orderId'])
    orders = client.get_all_orders(PAIR3)
    for order in orders:
        client.cancel_order(order['orderId'])


# time.sleep(5)
# bm.stop_socket(bnbbtc_conn_key)
# bm.stop_socket(ethbtc_conn_key)
# bm.stop_socket(bnbeth_conn_key)
#
# sys.exit(0)

query_initial_coin_balances()
launch_socket_listeners()

total_return = 0.0
exception_count = 0

print('find some trades')
while True:
    try:

        # calculate balance of each coin
        start_coin1_balance = 0.0
        start_coin2_balance = 0.0
        start_coin3_balance = 0.0
        start_bnb_balance = 0.0
        start_coin1_value = 0.0
        start_coin2_value = 0.0
        start_coin3_value = 0.0
        start_bnb_value = 0.0
        for coin in balance_book:
            if coin == COIN1:
                start_coin1_balance = balance_book[coin]
            elif coin == COIN2:
                start_coin2_balance = balance_book[coin]
            elif coin == COIN3:
                start_coin3_balance = balance_book[coin]
            elif coin == 'BNB':
                start_bnb_balance = balance_book[coin]

        # calculate the value of each coin in dollars
        if COIN1+'USDT' in raw_order_book:
            coin1_price = raw_order_book[COIN1+'USDT'].ask
        if COIN2+'USDT' in raw_order_book:
            coin2_price = raw_order_book[COIN2+'USDT'].ask
        if COIN3+'USDT' in raw_order_book:
            coin3_price = raw_order_book[COIN3+'USDT'].ask
        if 'BNBUSDT' in raw_order_book:
            bnb_price = raw_order_book['BNBUSDT'].ask
        # USDT prices are always 1.0 coin per dollar
        if COIN1 == 'USDT':
            coin1_price = 1.0
        elif COIN2 == 'USDT':
            coin2_price = 1.0
        elif COIN3 == 'USDT':
            coin3_price = 1.0

        start_coin1_value = start_coin1_balance * coin1_price
        start_coin2_value = start_coin2_balance * coin2_price
        start_coin3_value = start_coin3_balance * coin3_price
        start_bnb_value = start_bnb_balance * bnb_price

        print(COIN1 + ' starting balance:', start_coin1_balance)
        print(COIN2 + ' starting balance:', start_coin2_balance)
        print(COIN3 + ' starting balance:', start_coin3_balance)
        print('BNB starting balance:', start_bnb_balance)

        if start_bnb_value < 0.001 * (start_coin1_value+start_coin2_value+start_coin3_value):
            order = client.order_market_buy(symbol='BNBUSDT', quantity=2.00)
            # TODO: Log this somehow
            time.sleep(0.5)
            continue

        base_quantity = start_coin1_balance
        base_value = base_quantity * coin1_price

        if start_coin2_value < base_value:
            # print(COIN2 + ' value is too low')
            base_quantity = start_coin2_value / coin1_price
            base_value = start_coin2_value
        if start_coin3_value < base_value:
            # print(COIN3 + ' value is too low')
            base_quantity = start_coin3_value / coin1_price
            base_value = start_coin3_value

        base_quantity *= 0.8
        print('start base quantity: ', base_quantity)

        # adjust eth/btc quantities to re-balance funds if necessary.
        delta_coin1 = 0.0
        delta_coin2 = 0.0
        delta_coin3 = 0.0
        average_value = (start_coin1_value + start_coin2_value + start_coin3_value) / 3.0
        if abs(start_coin1_value - average_value) > 0.05 * average_value:
            delta_coin1 = (average_value - start_coin1_value) / (2*coin1_price)
        if abs(start_coin2_value - average_value) > 0.05 * average_value:
            delta_coin2 = (average_value - start_coin2_value) / (2*coin2_price)
        if abs(start_coin3_value - average_value) > 0.05 * average_value:
            delta_coin3 = (average_value - start_coin3_value) / (2*coin3_price)
        print('average value: ', average_value)
        print(COIN1 + ' value: ', start_coin1_value)
        print(COIN2 + ' value: ', start_coin2_value)
        print(COIN3 + ' value: ', start_coin3_value)
        print('delta ' + COIN1 + ': ', delta_coin1)
        print('delta ' + COIN2 + ': ', delta_coin2)
        print('delta ' + COIN3 + ': ', delta_coin3)

        order_start_time = datetime.utcnow().isoformat()

        # print(PAIR1 + ' bid: ', raw_order_book[PAIR1].bid, ' ask: ', raw_order_book[PAIR1].ask)
        # print(PAIR2 + ' bid: ', raw_order_book[PAIR2].bid, ' ask: ', raw_order_book[PAIR2].ask)
        # print(PAIR3 + ' bid: ', raw_order_book[PAIR3].bid, ' ask: ', raw_order_book[PAIR3].ask)

        # pick a price in the middle of the spread and see if that works for arbitrage
        trade_order_book = build_trade_order_book()

        coin1_per_coin2, coin2_per_coin1 = calculate_coin_ratio(COIN1, COIN2)
        coin1_per_coin3, coin3_per_coin1 = calculate_coin_ratio(COIN1, COIN3)
        coin2_per_coin3, coin3_per_coin2 = calculate_coin_ratio(COIN2, COIN3)

        print(COIN1 + '_per_' + COIN3 + ': ', coin1_per_coin3, coin3_per_coin1)
        print(COIN3 + '_per_' + COIN2 + ': ', coin3_per_coin2, coin2_per_coin3)
        print(COIN2 + '_per_' + COIN1 + ': ', coin2_per_coin1, coin1_per_coin2)

        forward_arbitrage = coin1_per_coin3 * coin3_per_coin2 * coin2_per_coin1
        reverse_arbitrage = coin1_per_coin2 * coin2_per_coin3 * coin3_per_coin1

        print('forward: ', forward_arbitrage)
        print('reverse: ', reverse_arbitrage)

        coin2_quantity = 0.0
        coin3_quantity = 0.0
        coin1_result = 0.0
        pair3_price = 0.0
        pair2_price = 0.0
        pair1_price = 0.0
        pair3_order = None
        pair2_order = None
        pair1_order = None
        canceled_pair1_order = None
        canceled_pair2_order = None
        canceled_pair3_order = None
        found_order = True

        if forward_arbitrage > reverse_arbitrage and forward_arbitrage > THRESHOLD:
            print('doing forward arbitrage')
            coin2_quantity, coin3_quantity, coin1_result = quick_calc(base_quantity,
                                                                      coin2_per_coin1,
                                                                      coin3_per_coin2,
                                                                      coin1_per_coin3)
            pair1_price = PRICE_FORMAT[PAIR1] % trade_order_book[PAIR1].bid
            pair2_price = PRICE_FORMAT[PAIR2] % trade_order_book[PAIR2].ask
            pair3_price = PRICE_FORMAT[PAIR3] % trade_order_book[PAIR3].ask
            print(PAIR1 + ' price: ', pair1_price)
            print(PAIR2 + ' price: ', pair2_price)
            print(PAIR3 + ' price: ', pair3_price)

            adjusted_coin1_quantity = base_quantity - delta_coin1
            adjusted_coin1_quantity += delta_coin2 * coin1_per_coin2
            adjusted_coin1_quantity = min(start_coin1_balance, adjusted_coin1_quantity)
            adjusted_coin2_quantity = coin2_quantity - delta_coin2
            adjusted_coin2_quantity += delta_coin3 * coin2_per_coin3
            adjusted_coin2_quantity = min(start_coin2_balance, adjusted_coin2_quantity)
            adjusted_coin3_quantity = coin3_quantity - delta_coin3
            adjusted_coin3_quantity += delta_coin1 * coin3_per_coin1
            adjusted_coin3_quantity = min(start_coin3_balance, adjusted_coin3_quantity)

            print(COIN1 + ': ', base_quantity, adjusted_coin1_quantity)
            print(COIN2 + ': ', coin2_quantity, adjusted_coin2_quantity)
            print(COIN3 + ': ', coin3_quantity, adjusted_coin3_quantity)

            if adjusted_coin1_quantity > 0:
                pair2_order = convert_coins(COIN1, COIN2, adjusted_coin1_quantity)
            if adjusted_coin2_quantity > 0:
                pair3_order = convert_coins(COIN2, COIN3, adjusted_coin2_quantity)
            if adjusted_coin3_quantity > 0:
                pair1_order = convert_coins(COIN3, COIN1, adjusted_coin3_quantity)
        elif reverse_arbitrage > forward_arbitrage and reverse_arbitrage > THRESHOLD:
            print('doing reverse arbitrage')
            coin3_quantity, coin2_quantity, coin1_result = quick_calc(base_quantity,
                                                                      coin3_per_coin1,
                                                                      coin2_per_coin3,
                                                                      coin1_per_coin2)
            pair1_price = PRICE_FORMAT[PAIR1] % trade_order_book[PAIR1].ask
            pair2_price = PRICE_FORMAT[PAIR2] % trade_order_book[PAIR2].bid
            pair3_price = PRICE_FORMAT[PAIR3] % trade_order_book[PAIR3].bid
            print(PAIR1 + ' price: ', pair1_price)
            print(PAIR2 + ' price: ', pair2_price)
            print(PAIR3 + ' price: ', pair3_price)

            adjusted_coin1_quantity = base_quantity - delta_coin1
            adjusted_coin1_quantity += delta_coin3 * coin1_per_coin3
            adjusted_coin1_quantity = min(start_coin1_balance, adjusted_coin1_quantity)
            adjusted_coin2_quantity = coin2_quantity - delta_coin2
            adjusted_coin2_quantity += delta_coin1 * coin2_per_coin1
            adjusted_coin2_quantity = min(start_coin2_balance, adjusted_coin2_quantity)
            adjusted_coin3_quantity = coin3_quantity - delta_coin3
            adjusted_coin3_quantity += delta_coin2 * coin3_per_coin2
            adjusted_coin3_quantity = min(start_coin3_balance, adjusted_coin3_quantity)

            print(COIN1 + ': ', base_quantity, adjusted_coin1_quantity)
            print(COIN2 + ': ', coin2_quantity, adjusted_coin2_quantity)
            print(COIN3 + ': ', coin3_quantity, adjusted_coin3_quantity)

            if adjusted_coin1_quantity > 0:
                pair1_order = convert_coins(COIN1, COIN3, adjusted_coin1_quantity)
            if adjusted_coin3_quantity > 0:
                pair3_order = convert_coins(COIN3, COIN2, adjusted_coin3_quantity)
            if adjusted_coin2_quantity > 0:
                pair2_order = convert_coins(COIN2, COIN1, adjusted_coin2_quantity)
        else:
            found_order = False
            print('no opportunity')

        # print(PAIR1 + ': ', pair1_order)
        # print(PAIR2 + ': ', pair2_order)
        # print(PAIR3 + ': ', pair3_order)
        start_time = time.time()
        check_count = 0
        if found_order:
            if pair1_order is None:
                pair1_order = EMPTY_ORDER
            if pair2_order is None:
                pair2_order = EMPTY_ORDER
            if pair3_order is None:
                pair3_order = EMPTY_ORDER
            while (pair1_order['status'] not in ['FILLED', 'None']
                   or pair2_order['status'] not in ['FILLED', 'None']
                   or pair3_order['status'] not in ['FILLED', 'None'])\
                  and start_time + 25 > time.time():
                print_order_status(pair1_order, pair2_order, pair3_order)
                #print('Status: ', pair1_order['status'], pair2_order['status'], pair3_order['status'])
                check_count += 1
                time.sleep(3)
                if pair1_order != EMPTY_ORDER and pair1_order['status'] != 'FILLED':
                    pair1_order = client.get_order(symbol=PAIR1, orderId=pair1_order['orderId'])
                    # if check_count % 4 == 0 and pair1_order['status'] != 'FILLED':
                    #     # reset the price
                    #     pair1_order = update_order(pair1_order, base_quantity, check_count / 4)
                if pair2_order != EMPTY_ORDER and pair2_order['status'] != 'FILLED':
                    pair2_order = client.get_order(symbol=PAIR2, orderId=pair2_order['orderId'])
                    # if check_count % 4 == 0 and pair2_order['status'] != 'FILLED':
                    #     # reset the price
                    #     pair2_order = update_order(pair2_order, coin2_quantity, check_count/4)
                if pair3_order != EMPTY_ORDER and pair3_order['status'] != 'FILLED':
                    pair3_order = client.get_order(symbol=PAIR3, orderId=pair3_order['orderId'])
                    # if check_count % 4 == 0 and pair3_order['status'] != 'FILLED':
                    #     # reset the price
                    #     pair3_order = update_order(pair3_order, base_quantity, check_count / 4)
            if pair1_order != EMPTY_ORDER and pair1_order['status'] != 'FILLED':
                print('cancel pair1_order')
                cancel_order(pair1_order)
            if pair2_order != EMPTY_ORDER and pair2_order['status'] != 'FILLED':
                print('cancel pair2_order')
                cancel_order(pair2_order)
            if pair3_order != EMPTY_ORDER and pair3_order['status'] != 'FILLED':
                print('cancel pair3_order')
                cancel_order(pair3_order)
            # give the system 1 second for balances to be updated
            time.sleep(1)

        order_end_time = datetime.utcnow().isoformat()

        end_coin1_balance = 0.0
        end_coin3_balance = 0.0
        end_coin2_balance = 0.0
        end_bnb_balance = 0.0
        for coin in balance_book:
            if coin == COIN1:
                end_coin1_balance = balance_book[coin]
            elif coin == COIN2:
                end_coin2_balance = balance_book[coin]
            elif coin == COIN3:
                end_coin3_balance = balance_book[coin]
            elif coin == 'BNB':
                end_bnb_balance = balance_book[coin]
        end_coin1_value = end_coin1_balance * coin1_price
        end_coin2_value = end_coin2_balance * coin2_price
        end_coin3_value = end_coin3_balance * coin3_price
        end_bnb_value = end_bnb_balance * bnb_price

        start_total_value = start_coin1_value+start_coin2_value+start_coin3_value+start_bnb_value
        end_total_value = end_coin1_value+end_coin2_value+end_coin3_value+end_bnb_value
        final_return = end_total_value-start_total_value
        total_return += final_return
        total_coin1_balance = end_coin1_balance + (end_coin2_balance * coin1_per_coin2) + (end_coin3_balance * coin1_per_coin3)
        total_coin2_balance = (end_coin1_balance * coin2_per_coin1) + end_coin2_balance + (end_coin3_balance * coin2_per_coin3)
        total_coin3_balance = (end_coin1_balance * coin3_per_coin1) + (end_coin2_balance * coin3_per_coin2) + end_coin3_balance

        if found_order:
            print(COIN1 + ' ending diff:', end_coin1_balance - start_coin1_balance)
            print(COIN2 + ' ending diff:', end_coin2_balance - start_coin2_balance)
            print(COIN3 + ' ending diff:', end_coin3_balance - start_coin3_balance)
            print('BNB ending diff:', end_bnb_balance - start_bnb_balance)

            if pair1_order != EMPTY_ORDER:
                pair1_check_order = client.get_order(symbol=PAIR1, orderId=pair1_order['orderId'])
                print(PAIR1 + ' order: ', pair1_check_order)
            else:
                pair1_check_order = EMPTY_ORDER
                print(PAIR1 + ' order: None')
            if pair2_order != EMPTY_ORDER:
                pair2_check_order = client.get_order(symbol=PAIR2, orderId=pair2_order['orderId'])
                print(PAIR2 + ' order: ', pair2_check_order)
            else:
                pair2_check_order = EMPTY_ORDER
                print(PAIR2 + ' order: None')
            if pair3_order != EMPTY_ORDER:
                pair3_check_order = client.get_order(symbol=PAIR3, orderId=pair3_order['orderId'])
                print(PAIR3 + ' order: ', pair3_check_order)
            else:
                pair3_check_order = EMPTY_ORDER
                print(PAIR3 + ' order: None')
            # print(PAIR1 + ' canceled order: ', canceled_pair1_order)
            # print(PAIR2 + ' canceled order: ', canceled_pair2_order)
            # print(PAIR3 + ' canceled order: ', canceled_pair3_order)

            # TODO: this needs to be updated now that orders/bid/ask/qty can change from
            # their initial values.
            log_list = [order_start_time, order_end_time, PAIR1, PAIR2, PAIR3, final_return,
                        raw_order_book[PAIR1].bid, raw_order_book[PAIR1].ask, raw_order_book[PAIR2].bid, raw_order_book[PAIR2].ask,
                        raw_order_book[PAIR3].bid, raw_order_book[PAIR3].ask, FEE, THRESHOLD, forward_arbitrage, reverse_arbitrage,
                        base_quantity, coin2_quantity, coin3_quantity, coin1_result,
                        pair1_check_order['orderId'], pair1_check_order['price'], pair1_check_order['origQty'],
                        pair1_check_order['executedQty'], pair1_check_order['status'], pair1_check_order['side'],
                        pair2_check_order['orderId'], pair2_check_order['price'], pair2_check_order['origQty'],
                        pair2_check_order['executedQty'], pair2_check_order['status'], pair2_check_order['side'],
                        pair3_check_order['orderId'], pair3_check_order['price'], pair3_check_order['origQty'],
                        pair3_check_order['executedQty'], pair3_check_order['status'], pair3_check_order['side'],
                        start_coin1_balance, end_coin1_balance, start_coin1_value, end_coin1_value,
                        start_coin2_balance, end_coin2_balance, start_coin2_value, end_coin2_value,
                        start_coin3_balance, end_coin3_balance, start_coin3_value, end_coin3_value,
                        start_bnb_balance, end_bnb_balance, end_bnb_balance - start_bnb_balance,
                        start_bnb_value, end_bnb_value, end_bnb_value - start_bnb_value,
                        delta_coin1, delta_coin2, delta_coin3, start_total_value, end_total_value,
                        total_coin1_balance, total_coin2_balance, total_coin3_balance]
            log_string = ','.join(str(x) for x in log_list)
            print('log line: ', log_string)
            order_logger.info(log_string)

        # print(COIN1 + ' gain: ', end_coin1_value - start_coin1_value)
        # print(COIN2 + ' gain: ', end_coin2_value - start_coin2_value)
        # print(COIN3 + ' gain: ', end_coin3_value - start_coin3_value)
        print('total start: ', start_total_value, 'total end: ', end_total_value)
        print('total ' + COIN1 + ': ', total_coin1_balance, 'total ' + COIN2 + ': ', total_coin2_balance, 'total ' + COIN3 + ': ', total_coin3_balance)
        print('return: ', final_return, total_return)
        if found_order:
            # pause a short bit so we can read the results
            # long term, this can be removed
            time.sleep(1)
        time.sleep(1)
        timestamp_exceptions = 0
    except exceptions.BinanceAPIException as e:
        exception_logger.error('Time: ' + datetime.utcnow().isoformat())
        exception_logger.error(e)
        if e.code == -1021:
            exception_logger.info('Timestamp error, pausing and trying again')
            print('Timestamp error code: ', e)
            print('Pausing and trying again')
            exception_count += 1
            if exception_count >= 3:
                # this exception keeps showing up so something must be wrong.  cancel
                # all orders and re-raise the exception
                cancel_all_orders()
                raise e
            time.sleep(3)
        elif e.code == -1001:
            exception_logger.error('Disconnect error, pausing and reconnecting')
            print('Disconnected, pause and reconnect', e)
            exception_count += 1
            if exception_count >= 3:
                # too many exceptions are occurring so something must be wrong.  shutdown
                # everything.
                cancel_all_orders()
                raise e
            shutdown_socket_listeners()
            time.sleep(3)
            launch_socket_listeners()
    except requests.exceptions.ReadTimeout as e:
        exception_logger.error('Disconnect error, pausing and reconnecting')
        print('Disconnected, pause and reconnect', e)
        exception_count += 1
        if exception_count >= 3:
            # too many exceptions are occurring so something must be wrong.  shutdown
            # everything.
            raise e
        time.sleep(3)
        client = Client(api_key, api_secret)
        bm = BinanceSocketManager(client)
        launch_socket_listeners()
    except Exception as e:
        print('Exitting on exception: ', e)
        exception_logger.error('Time: ' + datetime.utcnow().isoformat())
        exception_logger.error(e)
        shutdown_socket_listeners()
        raise e
