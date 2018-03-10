from binance.client import Client
from binance.websockets import BinanceSocketManager
import sys
import time
from binance import exceptions
import logging
from datetime import datetime, timedelta
import requests
import traceback
import gc
import copy

HALF_DAY = timedelta(hours=12)
ONE_DAY = timedelta(days=1)
FORWARD = 1
REVERSE = 0


class OrderBook:
    bid = 0
    ask = sys.maxsize


class BinanceArbitrage:
    api_key = 'RO0yfaBhlsb6rRZ3eAajt9Ptx347izGlfihXOskGhnk1NFcMVn4en7uTdtHAFfgD'
    api_secret = 'suH3HixQOlGKCeV4vqA8eEhU1lFJgJQuexzZIomkJJ6JUwOnWk8ugLDdVq2XJBU7'

    socket_start_time = None
    log_start_time = None

    exception_logger = None
    order_logger = None
    transaction_logger = None

    client = Client(api_key, api_secret)
    bm = BinanceSocketManager(client)

    bnbbtc_conn_key = None
    ethbtc_conn_key = None
    bnbeth_conn_key = None
    # neobtc_conn_key = None
    # neoeth_conn_key = None
    btcusdt_conn_key = None
    ethusdt_conn_key = None
    bnbusdt_conn_key = None
    account_conn_key = None

    EMPTY_ORDER = {'status': 'None',
                   'orderId': 'None',
                   'price': 0.0,
                   'origQty': 0.0,
                   'executedQty': 0.0,
                   'side': 'None'}

    # pair priorities USDT always comes last in a pair.  BTC always comes after all
    # coins other than USDT.  ETH comes after all coins other than USDT and BTC.
    # also, pair should go COIN1/COIN3, COIN2/COIN1, COIN2/COIN3
    COIN1 = 'BTC'
    COIN2 = 'ETH'
    COIN3 = 'USDT'
    PAIR1 = 'BTCUSDT'
    PAIR2 = 'ETHBTC'
    PAIR3 = 'ETHUSDT'
    PAIR_LIST = [PAIR1, PAIR2, PAIR3]

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
                        'ETHBTC': 0.95,
                        'BNBETH': 0.4,
                        'NEOBTC': 0.5,
                        'NEOETH': 0.5,
                        'BTCUSDT': 0.95,
                        'ETHUSDT': 0.65}
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
    THRESHOLD = 1.0021#16 # + (4 * FEE)
    TOPOFF_THRESHOLD = 1.0015
    BNB_QUANTITY = 6.0

    raw_order_book_timestamp = None
    raw_order_book = {'ETHBTC': OrderBook(),
                      'BTCUSDT': OrderBook(),
                      'ETHUSDT': OrderBook(),
                      'BNBUSDT': OrderBook()}

    balance_book = {'timestamp': None,
                    'locked': False,
                    'BNB': {'free': 0.0, 'locked': 0.0},
                    'ETH': {'free': 0.0, 'locked': 0.0},
                    'BTC': {'free': 0.0, 'locked': 0.0},
                    'USDT': {'free': 0.0, 'locked': 0.0}}

    trade_order_book = {}

    total_return = 0.0
    all_time_high = 0.0


    # def process_bnbbtc_depth_message(self, msg):
    #     self.raw_order_book['BNBBTC'].bid = float(msg['bids'][0][0])
    #     self.raw_order_book['BNBBTC'].ask = float(msg['asks'][0][0])
    #     self.raw_order_book_timestamp = datetime.utcnow()
    #     # print('Raw BNBBTC timestamp: ', self.raw_order_book_timestamp.isoformat())


    def process_ethbtc_depth_message(self, msg):
        #print('.')
        self.raw_order_book['ETHBTC'].bid = float(msg['bids'][0][0])
        self.raw_order_book['ETHBTC'].ask = float(msg['asks'][0][0])
        self.raw_order_book_timestamp = datetime.utcnow()
        # print('Raw ETHBTC timestamp: ', self.raw_order_book_timestamp.isoformat())


    # def process_bnbeth_depth_message(self, msg):
    #     self.raw_order_book['BNBETH'].bid = float(msg['bids'][0][0])
    #     self.raw_order_book['BNBETH'].ask = float(msg['asks'][0][0])
    #     self.raw_order_book_timestamp = datetime.utcnow()
    #     # print('Raw BNBETH timestamp: ', self.raw_order_book_timestamp.isoformat())


    # def process_neobtc_depth_message(self, msg):
    #     self.raw_order_book['NEOBTC'].bid = float(msg['bids'][0][0])
    #     self.raw_order_book['NEOBTC'].ask = float(msg['asks'][0][0])
    #     self.raw_order_book_timestamp = datetime.utcnow()
    #     # print('Raw NEOBTC timestamp: ', self.raw_order_book_timestamp.isoformat())


    # def process_neoeth_depth_message(self, msg):
    #     self.raw_order_book['NEOETH'].bid = float(msg['bids'][0][0])
    #     self.raw_order_book['NEOETH'].ask = float(msg['asks'][0][0])
    #     self.raw_order_book_timestamp = datetime.utcnow()
    #     # print('Raw NEOETH timestamp: ', self.raw_order_book_timestamp.isoformat())


    def process_btcusdt_depth_message(self, msg):
        self.raw_order_book['BTCUSDT'].bid = float(msg['bids'][0][0])
        self.raw_order_book['BTCUSDT'].ask = float(msg['asks'][0][0])
        self.raw_order_book_timestamp = datetime.utcnow()
        # print('Raw BTCUSDT timestamp: ', self.raw_order_book_timestamp.isoformat())


    def process_ethusdt_depth_message(self, msg):
        self.raw_order_book['ETHUSDT'].bid = float(msg['bids'][0][0])
        self.raw_order_book['ETHUSDT'].ask = float(msg['asks'][0][0])
        self.raw_order_book_timestamp = datetime.utcnow()
        # print('Raw ETHUSDT timestamp: ', self.raw_order_book_timestamp.isoformat())


    def process_bnbusdt_depth_message(self, msg):
        self.raw_order_book['BNBUSDT'].bid = float(msg['bids'][0][0])
        self.raw_order_book['BNBUSDT'].ask = float(msg['asks'][0][0])
        self.raw_order_book_timestamp = datetime.utcnow()
        # print('Raw BNBUSDT timestamp: ', self.raw_order_book_timestamp.isoformat())


    def process_account_message(self, msg):
        #print('user stream message: ', msg)
        if 'e' in msg and msg['e'] != 'outboundAccountInfo':
            # we only care about account info for now
            return
        if 'B' not in msg:
            # the outboundAccountInfo message should have balances
            return
        self.balance_book['locked'] = False
        for asset in msg['B']:
            if asset['a'] in self.balance_book:
                self.balance_book[asset['a']] = {'free': float(asset['f']),
                                                 'locked': float(asset['l'])}
                if float(asset['l']) > 0:
                    self.balance_book['locked'] = True
        self.balance_book['timestamp'] = datetime.utcnow()


    def update_order(self, order, check_level):
        if order['status'] == 'FILLED':
            return None

        new_order = None
        price = 0.0

        order_quantity = float(order['origQty'])
        executed_quantity = float(order['executedQty'])
        if (order_quantity - executed_quantity) < 10*self.MIN_AMOUNT[order['symbol']]:
            # May hit MIN NOTIONAL error if we try to re-submit the order for the
            # remaining amount.  Instead, let it ride, we shouldn't lose too much.
            return order
        new_quantity = round(order_quantity - executed_quantity,
                             self.QUANTITY_PRECISION[order['symbol']])

        # don't re-place the order if the price isn't going to change.  we will just
        # lose our place in line in the order book and damage our fills per order+cancels
        # metrics.
        if order['side'] == 'BUY':
            if self.raw_order_book[order['symbol']].bid == float(order['price']):
                return order
            price = self.raw_order_book[order['symbol']].bid
            if check_level >= 2:
                price = round(price + self.TICK[order['symbol']], self.PRICE_PRECISION[order['symbol']])
        elif order['side'] == 'SELL':
            if self.raw_order_book[order['symbol']].ask == float(order['price']):
                return order
            price = self.raw_order_book[order['symbol']].ask
            if check_level >= 2:
                price = round(price - self.TICK[order['symbol']], self.PRICE_PRECISION[order['symbol']])

        if price * new_quantity < self.MIN_NOTIONAL[order['symbol']]:
            print('Value under min notional')
            print('Price: ', price, 'quantity: ', new_quantity, 'min: ', self.MIN_NOTIONAL[order['symbol']])
            return order

        try:
            self.client.cancel_order(symbol=order['symbol'], orderId=order['orderId'])
        except exceptions.BinanceAPIException as e:
            if e.message == 'UNKNOWN_ORDER' or e.code == -2011:
                print('Order already filled')
                return
            else:
                self.exception_logger.info('Time: ' + datetime.utcnow().isoformat())
                self.exception_logger.info(traceback.format_exc())
                raise e
        if order['side'] == 'BUY':
            print('updating bid to: ', price, new_quantity)
            new_order = self.client.order_limit_buy(symbol=order['symbol'],
                                                    price=str(price),
                                                    quantity=new_quantity)
        elif order['side'] == 'SELL':
            print('updating ask to: ', price, new_quantity)
            new_order = self.client.order_limit_sell(symbol=order['symbol'],
                                                     price=str(price),
                                                     quantity=new_quantity)
        return new_order


    def cancel_order(self, order):
        print('Canceled: ', order['symbol'])
        try:
            self.client.cancel_order(symbol=order['symbol'], orderId=order['orderId'])
        except exceptions.BinanceAPIException as e:
            if e.message == 'UNKNOWN_ORDER' or e.code == -2011:
                print('Order already filled')
            else:
                self.exception_logger.error('Time: ' + datetime.utcnow().isoformat())
                self.exception_logger.error(traceback.format_exc())
                raise e


    @staticmethod
    def quick_calc(a_quantity, b_per_a, c_per_b, a_per_c):
        b_quantity = b_per_a * a_quantity
        c_quantity = c_per_b * b_quantity
        a_result = a_per_c * c_quantity

        return b_quantity, c_quantity, a_result


    def update_raw_order_book(self):
        order_book_tickers = self.client.get_orderbook_tickers()

        for symbol in order_book_tickers:
            if symbol['symbol'] in [self.PAIR1, self.PAIR2, self.PAIR3, 'BNBUSDT']:
                self.raw_order_book[symbol['symbol']].bid = float(symbol['bidPrice'])
                self.raw_order_book[symbol['symbol']].ask = float(symbol['askPrice'])
        # self.raw_order_book_timestamp = datetime.utcnow()


    def build_trade_order_book(self):
        trade_order = {self.PAIR1: OrderBook(),
                       self.PAIR2: OrderBook(),
                       self.PAIR3: OrderBook()}
        for pair in self.PAIR_LIST:
            spread = round((self.raw_order_book[pair].ask - self.raw_order_book[pair].bid) / self.TICK[pair], 0)

            # pick a price in the middle of the spread and see if that works for arbitrage
            trade_order[pair].bid = max(self.raw_order_book[pair].bid,
                                        self.raw_order_book[pair].ask - (
                                            self.SPREAD_THRESHOLD[pair] * spread * self.TICK[pair]))
            trade_order[pair].ask = min(self.raw_order_book[pair].ask,
                                        self.raw_order_book[pair].bid + (
                                            self.SPREAD_THRESHOLD[pair] * spread * self.TICK[pair]))

            trade_order[pair].bid = round(trade_order[pair].bid, self.PRICE_PRECISION[pair])
            trade_order[pair].ask = round(trade_order[pair].ask, self.PRICE_PRECISION[pair])

            print(pair + ' bid: ', self.PRICE_FORMAT[pair] % trade_order[pair].bid,
                  ' ask: ', self.PRICE_FORMAT[pair] % trade_order[pair].ask,
                  ' spread: ', spread)

        return trade_order


    def calculate_coin_ratio(self, coin1, coin2, order_book):
        if coin1+coin2 in [self.PAIR1, self.PAIR2, self.PAIR3]:
            coin1_per_coin2 = 1 / order_book[coin1+coin2].bid
            coin2_per_coin1 = order_book[coin1+coin2].ask
        elif coin2+coin1 in [self.PAIR1, self.PAIR2, self.PAIR3]:
            coin2_per_coin1 = 1 / order_book[coin2+coin1].bid
            coin1_per_coin2 = order_book[coin2+coin1].ask
        else:
            error_string = 'No pairs found for coins', coin1, coin2, 'in: ', self.PAIR1, self.PAIR2, self.PAIR3
            print(error_string)
            raise Exception(error_string)

        return coin1_per_coin2, coin2_per_coin1


    def calculate_raw_coin_ratio(self, coin1, coin2):
        if coin1+coin2 in [self.PAIR1, self.PAIR2, self.PAIR3]:
            coin1_per_coin2 = 1 / self.raw_order_book[coin1+coin2].ask
            coin2_per_coin1 = self.raw_order_book[coin1+coin2].bid
        elif coin2+coin1 in [self.PAIR1, self.PAIR2, self.PAIR3]:
            coin2_per_coin1 = 1 / self.raw_order_book[coin2+coin1].ask
            coin1_per_coin2 = self.raw_order_book[coin2+coin1].bid
        else:
            error_string = 'No pairs found for coins', coin1, coin2, 'in: ', self.PAIR1, self.PAIR2, self.PAIR3
            print(error_string)
            raise Exception(error_string)

        return coin1_per_coin2, coin2_per_coin1


    def convert_coins(self, coin1, coin2, quantity, order_book):
        # TODO: Need min amount in addition to min notional
        if coin1+coin2 in [self.PAIR1, self.PAIR2, self.PAIR3]:
            # sell
            pair = coin1+coin2
            price = self.PRICE_FORMAT[pair] % order_book[pair].ask
            adjusted_quantity = round(quantity, self.QUANTITY_PRECISION[pair])
            if order_book[pair].ask * adjusted_quantity < self.MIN_NOTIONAL[pair]:
                print('Value under min notional')
                print('Price: ', price, 'quantity: ', adjusted_quantity, 'min: ', self.MIN_NOTIONAL[pair])
                return None
            try:
                order = self.client.order_limit_sell(symbol=pair,
                                                     price=price,
                                                     quantity=round(adjusted_quantity,
                                                                    self.QUANTITY_PRECISION[pair]))
            except exceptions.BinanceAPIException as e:
                if e.code == -1013:
                    print('Value under min notional')
                    print('Price: ', price, 'quantity: ', adjusted_quantity, 'min: ', self.MIN_NOTIONAL[pair])
                    return None
                self.exception_logger.error('Time: ' + datetime.utcnow().isoformat())
                self.exception_logger.error('Exception placing an order')
                self.exception_logger.error('Coin1: ' + coin1 + ' Coin2: ' + coin2 + ' Quantity: ' + str(quantity))
                self.exception_logger.error('Sell Pair: ' + pair + ' Price: ' + str(price) + ' Adjusted Quantity: ' + str(adjusted_quantity))
                self.exception_logger.error(traceback.format_exc())
                print('Exception placing order', e)
                raise e
        elif coin2+coin1 in [self.PAIR1, self.PAIR2, self.PAIR3]:
            # buy
            pair = coin2+coin1
            price = self.PRICE_FORMAT[pair] % order_book[pair].bid
            coin1_per_coin2, coin2_per_coin1 = self.calculate_coin_ratio(coin1, coin2, order_book)
            adjusted_quantity = round(quantity * coin2_per_coin1, self.QUANTITY_PRECISION[pair])
            # convert coin1 quantity to coin2 quantity
            if order_book[pair].bid * adjusted_quantity < self.MIN_NOTIONAL[pair]:
                print('Value under min notional')
                print('Price: ', price, 'quantity: ', adjusted_quantity, 'min: ', self.MIN_NOTIONAL[pair])
                return None
            try:
                order = self.client.order_limit_buy(symbol=pair,
                                                    price=price,
                                                    quantity=round(adjusted_quantity,
                                                                   self.QUANTITY_PRECISION[pair]))
            except exceptions.BinanceAPIException as e:
                if e.code == -1013:
                    print('Value under min notional')
                    print('Price: ', price, 'quantity: ', adjusted_quantity, 'min: ', self.MIN_NOTIONAL[pair])
                    return None
                self.exception_logger.error('Time: ' + datetime.utcnow().isoformat())
                self.exception_logger.error('Exception placing an order')
                self.exception_logger.error('Coin1: ' + coin1 + ' Coin2: ' + coin2 + ' Quantity: ' + str(quantity))
                self.exception_logger.error('Buy Pair: ' + pair + ' Price: ' + str(price) + ' Adjusted Quantity: ' + str(adjusted_quantity))
                self.exception_logger.error(traceback.format_exc())
                print('Exception placing order', e)
                raise e
        else:
            order = None

        return order


    def market_convert_coins(self, coin1, coin2, quantity):
        # TODO: Need min amount in addition to min notional
        if coin1+coin2 in [self.PAIR1, self.PAIR2, self.PAIR3]:
            # sell
            pair = coin1+coin2
            price = self.PRICE_FORMAT[pair] % self.raw_order_book[pair].ask
            adjusted_quantity = round(quantity, self.QUANTITY_PRECISION[pair])
            if self.raw_order_book[pair].ask * adjusted_quantity < self.MIN_NOTIONAL[pair]:
                print('Value under min notional')
                print('Price: ', price, 'quantity: ', adjusted_quantity, 'min: ', self.MIN_NOTIONAL[pair])
                return None
            try:
                order = self.client.order_market_sell(symbol=pair,
                                                      quantity=round(adjusted_quantity,
                                                                     self.QUANTITY_PRECISION[pair]),
                                                      newOrderRespType='FULL')
                while 'status' not in order or order['status'] != 'FILLED':
                    time.sleep(0.5)
                    order = self.client.get_order(symbol=pair,
                                                  orderId=order['orderId'])

            except exceptions.BinanceAPIException as e:
                if e.code == -1013:
                    print('Value under min notional')
                    print('Price: ', price, 'quantity: ', adjusted_quantity, 'min: ', self.MIN_NOTIONAL[pair])
                    return None
                self.exception_logger.error('Time: ' + datetime.utcnow().isoformat())
                self.exception_logger.error('Exception placing an order')
                self.exception_logger.error('Coin1: ' + coin1 + ' Coin2: ' + coin2 + ' Quantity: ' + str(quantity))
                self.exception_logger.error('Sell Pair: ' + pair + ' Price: ' + str(price) + ' Adjusted Quantity: ' + str(adjusted_quantity))
                self.exception_logger.error(traceback.format_exc())
                print('Exception placing order', e)
                raise e
        elif coin2+coin1 in [self.PAIR1, self.PAIR2, self.PAIR3]:
            # buy
            pair = coin2+coin1
            price = self.PRICE_FORMAT[pair] % self.raw_order_book[pair].bid
            coin1_per_coin2, coin2_per_coin1 = self.calculate_raw_coin_ratio(coin1, coin2)
            adjusted_quantity = round(quantity * coin2_per_coin1, self.QUANTITY_PRECISION[pair])
            # convert coin1 quantity to coin2 quantity
            if self.raw_order_book[pair].bid * adjusted_quantity < self.MIN_NOTIONAL[pair]:
                print('Value under min notional')
                print('Price: ', price, 'quantity: ', adjusted_quantity, 'min: ', self.MIN_NOTIONAL[pair])
                return None
            try:
                order = self.client.order_market_buy(symbol=pair,
                                                     quantity=round(adjusted_quantity,
                                                                    self.QUANTITY_PRECISION[pair]),
                                                     newOrderRespType='FULL')
                while 'status' not in order or order['status'] != 'FILLED':
                    time.sleep(0.5)
                    order = self.client.get_order(symbol=pair,
                                                  orderId=order['orderId'])
            except exceptions.BinanceAPIException as e:
                if e.code == -1013:
                    print('Value under min notional')
                    print('Price: ', price, 'quantity: ', adjusted_quantity, 'min: ', self.MIN_NOTIONAL[pair])
                    return None
                self.exception_logger.error('Time: ' + datetime.utcnow().isoformat())
                self.exception_logger.error('Exception placing an order')
                self.exception_logger.error('Coin1: ' + coin1 + ' Coin2: ' + coin2 + ' Quantity: ' + str(quantity))
                self.exception_logger.error('Buy Pair: ' + pair + ' Price: ' + str(price) + ' Adjusted Quantity: ' + str(adjusted_quantity))
                self.exception_logger.error(traceback.format_exc())
                print('Exception placing order', e)
                raise e
        else:
            order = None

        return order


    @staticmethod
    def print_order_status(pair1_order, pair2_order, pair3_order):
        status_string = 'Status:  '
        if pair1_order is None or pair1_order['status'] == 'None':
            status_string += '---  '
        elif pair1_order['status'] == 'FILLED':
            status_string += '100  '
        else:
            status_string += '{:3d}  '.format(int(100*float(pair1_order['executedQty'])/float(pair1_order['origQty'])))
        if pair2_order is None or pair2_order['status'] == 'None':
            status_string += '---  '
        elif pair2_order['status'] == 'FILLED':
            status_string += '100  '
        else:
            status_string += '{:3d}  '.format(int(100*float(pair2_order['executedQty'])/float(pair2_order['origQty'])))
        if pair3_order is None or pair3_order['status'] == 'None':
            status_string += '---  '
        elif pair3_order['status'] == 'FILLED':
            status_string += '100  '
        else:
            status_string += '{:3d}  '.format(int(100*float(pair3_order['executedQty'])/float(pair3_order['origQty'])))

        print(status_string)


    def query_coin_balances(self):
        result = self.client.get_account()
        self.balance_book[self.COIN1] = {'free': 0.0, 'locked': 0.0}
        self.balance_book[self.COIN2] = {'free': 0.0, 'locked': 0.0}
        self.balance_book[self.COIN3] = {'free': 0.0, 'locked': 0.0}
        self.balance_book['BNB'] = {'free': 0.0, 'locked': 0.0}
        self.balance_book['locked'] = False
        for asset in result['balances']:
            if asset['asset'] in [self.COIN1, self.COIN2, self.COIN3, 'BNB']:
                self.balance_book[asset['asset']]['free'] += float(asset['free'])
                if float(asset['locked']) > 0:
                    self.balance_book[asset['asset']]['locked'] += float(asset['locked'])
                    self.balance_book['locked'] = True
        self.balance_book['timestamp'] = datetime.utcnow()


    def start_logging(self):
        self.log_start_time = datetime.utcnow().date()

        self.order_logger = logging.getLogger('order_tracker')
        self.order_logger.setLevel(logging.DEBUG)
        self.exception_logger = logging.getLogger('exception_tracker')
        self.exception_logger.setLevel(logging.DEBUG)
        self.transaction_logger = logging.getLogger('transaction_tracker')
        self.transaction_logger.setLevel(logging.DEBUG)

        base = 'logs\\'
        order_log_file_name = '%sbinance_orders_%s.log' % (base, self.log_start_time.isoformat())
        exception_log_file_name = '%sbinance_exceptions_%s.log' % (base, self.log_start_time.isoformat())
        transaction_log_file_name = '%sbinance_transactions_%s.log' % (base, self.log_start_time.isoformat())
        order_log_file_handler = logging.FileHandler(order_log_file_name)
        order_log_file_handler.setLevel(logging.INFO)
        exception_log_file_handler = logging.FileHandler(exception_log_file_name)
        exception_log_file_handler.setLevel(logging.INFO)
        transaction_log_file_handler = logging.FileHandler(transaction_log_file_name)
        transaction_log_file_handler.setLevel(logging.INFO)

        # remove and existing log handlers and replace them with the ones we just created
        for handler in self.order_logger.handlers[:]:
            self.order_logger.removeHandler(handler)
        self.order_logger.addHandler(order_log_file_handler)
        for handler in self.exception_logger.handlers[:]:
            self.exception_logger.removeHandler(handler)
        self.exception_logger.addHandler(exception_log_file_handler)
        for handler in self.transaction_logger.handlers[:]:
            self.transaction_logger.removeHandler(handler)
        self.transaction_logger.addHandler(transaction_log_file_handler)


    def launch_socket_listeners(self):
        print('Launch socket listeners')
        self.bm = BinanceSocketManager(self.client)
        # start any sockets here, i.e a trade socket
        # self.bnbbtc_conn_key = self.bm.start_depth_socket('BNBBTC',
        #                                                   self.process_bnbbtc_depth_message,
        #                                                   depth=BinanceSocketManager.WEBSOCKET_DEPTH_5)
        self.ethbtc_conn_key = self.bm.start_depth_socket('ETHBTC',
                                                          self.process_ethbtc_depth_message,
                                                          depth=BinanceSocketManager.WEBSOCKET_DEPTH_5)
        # self.bnbeth_conn_key = self.bm.start_depth_socket('BNBETH',
        #                                                   self.process_bnbeth_depth_message,
        #                                                   depth=BinanceSocketManager.WEBSOCKET_DEPTH_5)
        # self.neobtc_conn_key = self.bm.start_depth_socket('NEOBTC',
        #                                                   self.process_neobtc_depth_message,
        #                                                   depth=BinanceSocketManager.WEBSOCKET_DEPTH_5)
        # self.neoeth_conn_key = self.bm.start_depth_socket('NEOETH',
        #                                                   self.process_neoeth_depth_message,
        #                                                   depth=BinanceSocketManager.WEBSOCKET_DEPTH_5)
        self.btcusdt_conn_key = self.bm.start_depth_socket('BTCUSDT',
                                                           self.process_btcusdt_depth_message,
                                                           depth=BinanceSocketManager.WEBSOCKET_DEPTH_5)
        self.ethusdt_conn_key = self.bm.start_depth_socket('ETHUSDT',
                                                           self.process_ethusdt_depth_message,
                                                           depth=BinanceSocketManager.WEBSOCKET_DEPTH_5)
        self.bnbusdt_conn_key = self.bm.start_depth_socket('BNBUSDT',
                                                           self.process_bnbusdt_depth_message,
                                                           depth=BinanceSocketManager.WEBSOCKET_DEPTH_5)
        self.account_conn_key = self.bm.start_user_socket(self.process_account_message)
        # then start the socket manager
        self.bm.start()
        self.socket_start_time = datetime.utcnow()

        # wait till we have data for all pairs
        print('initialize order book')
        counter = 0
        while self.raw_order_book['ETHBTC'].bid == 0 or \
                self.raw_order_book['BTCUSDT'].bid == 0 or \
                self.raw_order_book['ETHUSDT'].bid == 0 or \
                self.raw_order_book['BNBUSDT'].bid == 0:
            counter += 1
            if counter > 20:
                raise Exception('Socket listener error')

            print(self.raw_order_book['ETHBTC'].bid,
                  self.raw_order_book['BTCUSDT'].bid,
                  self.raw_order_book['ETHUSDT'].bid,
                  self.raw_order_book['BNBUSDT'].bid)
            time.sleep(1)


    def shutdown_socket_listeners(self):
        # self.bm.stop_socket(self.bnbbtc_conn_key)
        self.bm.stop_socket(self.ethbtc_conn_key)
        # self.bm.stop_socket(self.bnbeth_conn_key)
        # self.bm.stop_socket(self.neobtc_conn_key)
        # self.bm.stop_socket(self.neoeth_conn_key)
        self.bm.stop_socket(self.btcusdt_conn_key)
        self.bm.stop_socket(self.ethusdt_conn_key)
        self.bm.stop_socket(self.bnbusdt_conn_key)
        self.bm.stop_socket(self.account_conn_key)


    def cancel_all_orders(self):
        orders = self.client.get_open_orders(symbol=self.PAIR1)
        for order in orders:
            try:
                if order['status'] != 'FILLED':
                    self.client.cancel_order(symbol=self.PAIR1, orderId=order['orderId'])
            except exceptions.BinanceAPIException as e:
                # ignore unknown orders because it probably means the order was already
                # filled.
                if e.code != -2011:
                    raise e
        orders = self.client.get_open_orders(symbol=self.PAIR2)
        for order in orders:
            try:
                if order['status'] != 'FILLED':
                    self.client.cancel_order(symbol=self.PAIR2, orderId=order['orderId'])
            except exceptions.BinanceAPIException as e:
                # ignore unknown orders because it probably means the order was already
                # filled.
                if e.code != -2011:
                    raise e
        orders = self.client.get_open_orders(symbol=self.PAIR3)
        for order in orders:
            try:
                if order['status'] != 'FILLED':
                    self.client.cancel_order(symbol=self.PAIR3, orderId=order['orderId'])
            except exceptions.BinanceAPIException as e:
                # ignore unknown orders because it probably means the order was already
                # filled.
                if e.code != -2011:
                    raise e


    @staticmethod
    def calculate_coin_delta(start_value, average_value, price):
        if abs(start_value - average_value) > 0.05 * average_value:
            return (average_value - start_value) / (2 * price)
        else:
            return 0.0


    def update_transaction_log(self, transaction_list):
        for transaction in transaction_list:
            if transaction is not None and transaction['status'] is not 'None':
                try:
                    # uncomment this if the empty transactions become a problem
                    # if float(transaction['executedQty']) == 0.0:
                    #     continue
                    #
                    if 'time' in transaction:
                        transaction_time = transaction['time']
                    else:
                        transaction_time = transaction['transactTime']
                    if 'fills' not in transaction:
                        transaction['fills'] = [{'price': transaction['price'],
                                                 'qty': transaction['executedQty'],
                                                 'commission': self.FEE*float(transaction['executedQty']),
                                                 'tradeId': 0
                                                 }]
                    if 'memo' not in transaction:
                        transaction['memo'] = 'NO_MEMO'

                    for sub_transaction in transaction['fills']:
                        commission = '%.8f' % (float(sub_transaction['commission'])*float(sub_transaction['price']))
                        log_list = ['binance', 'v1.0', datetime.utcfromtimestamp(transaction_time/1000.0).isoformat(),
                                    transaction['symbol'],
                                    float(sub_transaction['price']), float(sub_transaction['qty']),
                                    float(transaction['origQty']), float(transaction['executedQty']),
                                    (float(transaction['executedQty'])/float(transaction['origQty'])),
                                    transaction['status'], transaction['side'], commission, transaction['memo'],
                                    transaction['orderId'], transaction['clientOrderId'], sub_transaction['tradeId']]
                        log_string = ','.join(str(x) for x in log_list)
                        print('log line: ', log_string)
                        self.transaction_logger.info(log_string)
                except Exception as e:
                    self.exception_logger.error('Time: ' + datetime.utcnow().isoformat())
                    self.exception_logger.error('Exception logging transaction: ', str(transaction))
                    self.exception_logger.error(traceback.format_exc())


    def refill_bnb(self, start_balance, all_transactions):
        price = {self.COIN1: 0.0, self.COIN2: 0.0, self.COIN3: 0.0, 'BNB': 0.0}
        start_value = {self.COIN1: 0.0, self.COIN2: 0.0, self.COIN3: 0.0, 'BNB': 0.0}

        for coin in price:
            if coin == 'USDT':
                price[coin] = 1.0
            else:
                price[coin] = self.raw_order_book[coin+'USDT'].ask

        for coin in start_value:
            start_value[coin] = start_balance[coin] * price[coin]
            # print(coin + ' value: ', start_value[coin])

        if start_value['BNB'] < 0.001 * (start_value[self.COIN1]+start_value[self.COIN2]+start_value[self.COIN3]):
            order = self.client.order_market_buy(symbol='BNBUSDT', quantity=2.00, newOrderRespType='FULL')
            while 'status' not in order or order['status'] != 'FILLED':
                time.sleep(0.5)
                order = self.client.get_order(symbol='BNBUSDT', orderId=order['orderId'])
            order['memo'] = 'FEE_PAYMENT'
            all_transactions.append(order)


    def fill_percent(self, pair1_order, pair2_order, pair3_order):
        if pair1_order != self.EMPTY_ORDER:
            pair1_executed = float(pair1_order['executedQty']) / float(pair1_order['origQty'])
        else:
            pair1_executed = 1.0
        if pair2_order != self.EMPTY_ORDER:
            pair2_executed = float(pair2_order['executedQty']) / float(pair2_order['origQty'])
        else:
            pair2_executed = 1.0
        if pair3_order != self.EMPTY_ORDER:
            pair3_executed = float(pair3_order['executedQty']) / float(pair3_order['origQty'])
        else:
            pair3_executed = 1.0
        return pair1_executed, pair2_executed, pair3_executed


    def check_arbitrage(self):
        # calculate balance of each coin
        start_balance = {self.COIN1: 0.0, self.COIN2: 0.0, self.COIN3: 0.0, 'BNB': 0.0}
        price = {self.COIN1: 0.0, self.COIN2: 0.0, self.COIN3: 0.0, 'BNB': 0.0}
        start_value = {self.COIN1: 0.0, self.COIN2: 0.0, self.COIN3: 0.0, 'BNB': 0.0}
        delta = {self.COIN1: 0.0, self.COIN2: 0.0, self.COIN3: 0.0}
        base_quantity = {self.COIN1: 0.0, self.COIN2: 0.0, self.COIN3: 0.0}
        adjusted_quantity = {self.COIN1: 0.0, self.COIN2: 0.0, self.COIN3: 0.0}
        coin_per_coin = {self.COIN1: {self.COIN2: 0.0, self.COIN3: 0.0},
                         self.COIN2: {self.COIN1: 0.0, self.COIN3: 0.0},
                         self.COIN3: {self.COIN1: 0.0, self.COIN2: 0.0}}

        all_transactions = []
        for coin in self.balance_book:
            if coin in ['timestamp', 'locked']:
                continue
            start_balance[coin] = self.balance_book[coin]['free']
            print(coin + ' starting balance:', start_balance[coin])

        self.refill_bnb(start_balance, all_transactions)

        # calculate the value of each coin in dollars
        self.update_raw_order_book()
        for coin in price:
            if coin == 'USDT':
                price[coin] = 1.0
            else:
                price[coin] = self.raw_order_book[coin+'USDT'].ask

        for coin in start_value:
            start_value[coin] = start_balance[coin] * price[coin]
            print(coin + ' value: ', start_value[coin])

        base_quantity[self.COIN1] = start_balance[self.COIN1]
        base_value = base_quantity[self.COIN1] * price[self.COIN1]

        if start_value[self.COIN2] < base_value:
            # coin2 value is too low
            base_quantity[self.COIN1] = start_value[self.COIN2] / price[self.COIN1]
            base_value = start_value[self.COIN2]
        if start_value[self.COIN3] < base_value:
            # coin3 value is too low
            base_quantity[self.COIN1] = start_value[self.COIN3] / price[self.COIN1]

        # don't use all our available coins
        base_quantity[self.COIN1] *= 0.8
        print('start base quantity: ', base_quantity[self.COIN1])

        # adjust eth/btc quantities to re-balance funds if necessary.
        average_value = (start_value[self.COIN1] + start_value[self.COIN2] + start_value[self.COIN3]) / 3.0
        print('average value: ', average_value)
        # only adjust the coin values by half so we can try to get a little actual arbitrage
        for coin in delta:
            delta[coin] = self.calculate_coin_delta(start_value[coin], average_value, price[coin])
            print('delta ' + coin + ': ', delta[coin])

        order_start_time = datetime.utcnow()

        # pick a price in the middle of the spread and see if that works for arbitrage
        self.trade_order_book = self.build_trade_order_book()
        original_order_book = copy.deepcopy(self.trade_order_book)
        original_raw_order_book = copy.deepcopy(self.raw_order_book)

        for pair in self.PAIR_LIST:
            print('raw', pair + ' bid: ', self.PRICE_FORMAT[pair] % self.raw_order_book[pair].bid,
                  ' ask: ', self.PRICE_FORMAT[pair] % self.raw_order_book[pair].ask)

        # coin1_per_coin2, coin2_per_coin1 = self.calculate_coin_ratio(self.COIN1, self.COIN2, original_order_book)
        # coin1_per_coin3, coin3_per_coin1 = self.calculate_coin_ratio(self.COIN1, self.COIN3, original_order_book)
        # coin2_per_coin3, coin3_per_coin2 = self.calculate_coin_ratio(self.COIN2, self.COIN3, original_order_book)

        coin_per_coin[self.COIN1][self.COIN2], coin_per_coin[self.COIN2][self.COIN1] = self.calculate_coin_ratio(self.COIN1, self.COIN2, original_order_book)
        coin_per_coin[self.COIN1][self.COIN3], coin_per_coin[self.COIN3][self.COIN1] = self.calculate_coin_ratio(self.COIN1, self.COIN3, original_order_book)
        coin_per_coin[self.COIN2][self.COIN3], coin_per_coin[self.COIN3][self.COIN2] = self.calculate_coin_ratio(self.COIN2, self.COIN3, original_order_book)

        # print(self.COIN1 + '_per_' + self.COIN3 + ': ', coin1_per_coin3, coin3_per_coin1)
        # print(self.COIN3 + '_per_' + self.COIN2 + ': ', coin3_per_coin2, coin2_per_coin3)
        # print(self.COIN2 + '_per_' + self.COIN1 + ': ', coin2_per_coin1, coin1_per_coin2)

        # old_forward_arbitrage = coin1_per_coin3 * coin3_per_coin2 * coin2_per_coin1
        # old_reverse_arbitrage = coin1_per_coin2 * coin2_per_coin3 * coin3_per_coin1
        forward_arbitrage = coin_per_coin[self.COIN1][self.COIN3] * coin_per_coin[self.COIN3][self.COIN2] * coin_per_coin[self.COIN2][self.COIN1]
        reverse_arbitrage = coin_per_coin[self.COIN1][self.COIN2] * coin_per_coin[self.COIN2][self.COIN3] * coin_per_coin[self.COIN3][self.COIN1]

        # print('forward: ', '%.5f' % old_forward_arbitrage, '%.5f' % forward_arbitrage)
        # print('reverse: ', '%.5f' % old_reverse_arbitrage, '%.5f' % reverse_arbitrage)
        print('gain: ',
              '%.3f' % ((max(forward_arbitrage, reverse_arbitrage)-1.0)*100),
              '  (%.3f' % ((forward_arbitrage-1.0)*100),
              '%.3f)' % ((reverse_arbitrage-1.0)*100))

        if forward_arbitrage > reverse_arbitrage:
            direction = FORWARD
            gain = forward_arbitrage
            start_coin = self.COIN1
            mid_coin = self.COIN3
            end_coin = self.COIN2
        else:
            direction = REVERSE
            gain = reverse_arbitrage
            start_coin = self.COIN1
            mid_coin = self.COIN2
            end_coin = self.COIN3

        pair1_order = self.EMPTY_ORDER
        pair2_order = self.EMPTY_ORDER
        pair3_order = self.EMPTY_ORDER
        updated_pair1_order = self.EMPTY_ORDER
        updated_pair2_order = self.EMPTY_ORDER
        updated_pair3_order = self.EMPTY_ORDER
        found_order = True

        order_timestamp = datetime.utcnow()
        if direction == FORWARD and gain > self.THRESHOLD:
            print('doing forward arbitrage')
            # base_quantity[self.COIN2], base_quantity[self.COIN3], coin1_result = self.quick_calc(base_quantity[self.COIN1],
            #                                                                coin2_per_coin1,
            #                                                                coin3_per_coin2,
            #                                                                coin1_per_coin3)
            #
            # adjusted_quantity[self.COIN1] = base_quantity[self.COIN1] - delta[self.COIN1]
            # adjusted_quantity[self.COIN1] += delta[self.COIN2] * coin1_per_coin2
            # adjusted_quantity[self.COIN1] = min(0.95*start_balance[self.COIN1], adjusted_quantity[self.COIN1])
            # adjusted_quantity[self.COIN2] = base_quantity[self.COIN2] - delta[self.COIN2]
            # adjusted_quantity[self.COIN2] += delta[self.COIN3] * coin2_per_coin3
            # adjusted_quantity[self.COIN2] = min(0.95*start_balance[self.COIN2], adjusted_quantity[self.COIN2])
            # adjusted_quantity[self.COIN3] = base_quantity[self.COIN3] - delta[self.COIN3]
            # adjusted_quantity[self.COIN3] += delta[self.COIN1] * coin3_per_coin1
            # adjusted_quantity[self.COIN3] = min(0.95*start_balance[self.COIN3], adjusted_quantity[self.COIN3])

            # if adjusted_quantity[self.COIN1] > 0:
            #     pair2_order = self.convert_coins(self.COIN1, self.COIN2, adjusted_quantity[self.COIN1], original_order_book)
            # if adjusted_quantity[self.COIN2] > 0:
            #     pair3_order = self.convert_coins(self.COIN2, self.COIN3, adjusted_quantity[self.COIN2], original_order_book)
            # if adjusted_quantity[self.COIN3] > 0:
            #     pair1_order = self.convert_coins(self.COIN3, self.COIN1, adjusted_quantity[self.COIN3], original_order_book)
            #
            # bq = {start_coin: base_quantity[self.COIN1]}
            base_quantity[end_coin], base_quantity[mid_coin], c1_result = self.quick_calc(base_quantity[start_coin],
                                                                    coin_per_coin[end_coin][start_coin],
                                                                    coin_per_coin[mid_coin][end_coin],
                                                                    coin_per_coin[start_coin][mid_coin])

            adjusted_quantity[start_coin] = base_quantity[start_coin] - delta[start_coin]
            adjusted_quantity[start_coin] += delta[end_coin] * coin_per_coin[start_coin][end_coin]
            adjusted_quantity[start_coin] = min(0.95*start_balance[start_coin], adjusted_quantity[start_coin])
            adjusted_quantity[end_coin] = base_quantity[end_coin] - delta[end_coin]
            adjusted_quantity[end_coin] += delta[mid_coin] * coin_per_coin[end_coin][mid_coin]
            adjusted_quantity[end_coin] = min(0.95*start_balance[end_coin], adjusted_quantity[end_coin])
            adjusted_quantity[mid_coin] = base_quantity[mid_coin] - delta[mid_coin]
            adjusted_quantity[mid_coin] += delta[start_coin] * coin_per_coin[mid_coin][start_coin]
            adjusted_quantity[mid_coin] = min(0.95*start_balance[mid_coin], adjusted_quantity[mid_coin])

            if adjusted_quantity[self.COIN1] > 0:
                pair2_order = self.convert_coins(self.COIN1, self.COIN2, adjusted_quantity[self.COIN1], original_order_book)
            if adjusted_quantity[self.COIN2] > 0:
                pair3_order = self.convert_coins(self.COIN2, self.COIN3, adjusted_quantity[self.COIN2], original_order_book)
            if adjusted_quantity[self.COIN3] > 0:
                pair1_order = self.convert_coins(self.COIN3, self.COIN1, adjusted_quantity[self.COIN3], original_order_book)

            print(self.COIN1 + ': ', base_quantity[self.COIN1], adjusted_quantity[self.COIN1])
            print(self.COIN2 + ': ', base_quantity[self.COIN2], adjusted_quantity[self.COIN2])
            print(self.COIN3 + ': ', base_quantity[self.COIN3], adjusted_quantity[self.COIN3])
        elif direction == REVERSE and gain > self.THRESHOLD:
            print('doing reverse arbitrage')
            # base_quantity[self.COIN3], base_quantity[self.COIN2], coin1_result = self.quick_calc(base_quantity[self.COIN1],
            #                                                                coin3_per_coin1,
            #                                                                coin2_per_coin3,
            #                                                                coin1_per_coin2)
            #
            # adjusted_quantity[self.COIN1] = base_quantity[self.COIN1] - delta[self.COIN1]
            # adjusted_quantity[self.COIN1] += delta[self.COIN3] * coin1_per_coin3
            # adjusted_quantity[self.COIN1] = min(0.95*start_balance[self.COIN1], adjusted_quantity[self.COIN1])
            # adjusted_quantity[self.COIN2] = base_quantity[self.COIN2] - delta[self.COIN2]
            # adjusted_quantity[self.COIN2] += delta[self.COIN1] * coin2_per_coin1
            # adjusted_quantity[self.COIN2] = min(0.95*start_balance[self.COIN2], adjusted_quantity[self.COIN2])
            # adjusted_quantity[self.COIN3] = base_quantity[self.COIN3] - delta[self.COIN3]
            # adjusted_quantity[self.COIN3] += delta[self.COIN2] * coin3_per_coin2
            # adjusted_quantity[self.COIN3] = min(0.95*start_balance[self.COIN3], adjusted_quantity[self.COIN3])

            # if adjusted_quantity[self.COIN1] > 0:
            #     pair1_order = self.convert_coins(self.COIN1, self.COIN3, adjusted_quantity[self.COIN1], original_order_book)
            # if adjusted_quantity[self.COIN3] > 0:
            #     pair3_order = self.convert_coins(self.COIN3, self.COIN2, adjusted_quantity[self.COIN3], original_order_book)
            # if adjusted_quantity[self.COIN2] > 0:
            #     pair2_order = self.convert_coins(self.COIN2, self.COIN1, adjusted_quantity[self.COIN2], original_order_book)
            #
            # bq = {start_coin: base_quantity[self.COIN1]}
            base_quantity[end_coin], base_quantity[mid_coin], c1_result = self.quick_calc(base_quantity[start_coin],
                                                                    coin_per_coin[end_coin][start_coin],
                                                                    coin_per_coin[mid_coin][end_coin],
                                                                    coin_per_coin[start_coin][mid_coin])

            adjusted_quantity[start_coin] = base_quantity[start_coin] - delta[start_coin]
            adjusted_quantity[start_coin] += delta[end_coin] * coin_per_coin[start_coin][end_coin]
            adjusted_quantity[start_coin] = min(0.95*start_balance[start_coin], adjusted_quantity[start_coin])
            adjusted_quantity[end_coin] = base_quantity[end_coin] - delta[end_coin]
            adjusted_quantity[end_coin] += delta[mid_coin] * coin_per_coin[end_coin][mid_coin]
            adjusted_quantity[end_coin] = min(0.95*start_balance[end_coin], adjusted_quantity[end_coin])
            adjusted_quantity[mid_coin] = base_quantity[mid_coin] - delta[mid_coin]
            adjusted_quantity[mid_coin] += delta[start_coin] * coin_per_coin[mid_coin][start_coin]
            adjusted_quantity[mid_coin] = min(0.95*start_balance[mid_coin], adjusted_quantity[mid_coin])

            if adjusted_quantity[self.COIN1] > 0:
                pair1_order = self.convert_coins(self.COIN1, self.COIN3, adjusted_quantity[self.COIN1], original_order_book)
            if adjusted_quantity[self.COIN3] > 0:
                pair3_order = self.convert_coins(self.COIN3, self.COIN2, adjusted_quantity[self.COIN3], original_order_book)
            if adjusted_quantity[self.COIN2] > 0:
                pair2_order = self.convert_coins(self.COIN2, self.COIN1, adjusted_quantity[self.COIN2], original_order_book)

            print(self.COIN1 + ': ', base_quantity[self.COIN1], adjusted_quantity[self.COIN1])
            print(self.COIN2 + ': ', base_quantity[self.COIN2], adjusted_quantity[self.COIN2])
            print(self.COIN3 + ': ', base_quantity[self.COIN3], adjusted_quantity[self.COIN3])
        else:
            found_order = False
            print('no opportunity')

        start_time = time.time()
        check_count = 0
        if found_order:
            if pair1_order is None:
                pair1_order = self.EMPTY_ORDER
            if pair2_order is None:
                pair2_order = self.EMPTY_ORDER
            if pair3_order is None:
                pair3_order = self.EMPTY_ORDER
            if direction == FORWARD:
                print(self.COIN3 + '->' + self.COIN1, self.COIN1 + '->' + self.COIN2, self.COIN2 + '->' + self.COIN3)
            else:
                print(self.COIN1 + '->' + self.COIN3, self.COIN2 + '->' + self.COIN1, self.COIN3 + '->' + self.COIN2)
            while (pair1_order['status'] not in ['FILLED', 'None']
                   or pair2_order['status'] not in ['FILLED', 'None']
                   or pair3_order['status'] not in ['FILLED', 'None'])\
                  and start_time + 45 > time.time():
                self.print_order_status(pair1_order, pair2_order, pair3_order)
                check_count += 1
                time.sleep(3)
                if pair1_order != self.EMPTY_ORDER and pair1_order['status'] != 'FILLED':
                    try:
                        pair1_order = self.client.get_order(symbol=self.PAIR1,
                                                            orderId=pair1_order['orderId'],
                                                            origClientOrderId=pair1_order['clientOrderId'])
                    except exceptions.BinanceAPIException as e:
                        if e.code == -2013:
                            self.exception_logger.warning('Time: ' + datetime.utcnow().isoformat())
                            self.exception_logger.warning('Error: ' + self.PAIR1 + ' Order doesnt exist')
                            self.exception_logger.warning('Original: ' + str(pair1_order))
                        raise e
                    if check_count % 4 == 0 and pair1_order['status'] != 'FILLED':
                        pass
                        # reset the price
                        # pair1_order = update_order(pair1_order, base_quantity[self.COIN1], check_count / 4)
                if pair2_order != self.EMPTY_ORDER and pair2_order['status'] != 'FILLED':
                    try:
                        pair2_order = self.client.get_order(symbol=self.PAIR2,
                                                            orderId=pair2_order['orderId'],
                                                            origClientOrderId=pair2_order['clientOrderId'])
                    except exceptions.BinanceAPIException as e:
                        if e.code == -2013:
                            self.exception_logger.warning('Time: ' + datetime.utcnow().isoformat())
                            self.exception_logger.warning('Error: ' + self.PAIR2 + ' Order doesnt exist')
                            self.exception_logger.warning('Original: ' + str(pair2_order))
                        raise e
                    # if check_count % 4 == 0 and pair2_order['status'] != 'FILLED':
                    #     # reset the price
                    #     pair2_order = update_order(pair2_order, base_quantity[self.COIN2], check_count/4)
                if pair3_order != self.EMPTY_ORDER and pair3_order['status'] != 'FILLED':
                    try:
                        pair3_order = self.client.get_order(symbol=self.PAIR3,
                                                            orderId=pair3_order['orderId'],
                                                            origClientOrderId=pair3_order['clientOrderId'])
                    except exceptions.BinanceAPIException as e:
                        if e.code == -2013:
                            self.exception_logger.warning('Time: ' + datetime.utcnow().isoformat())
                            self.exception_logger.warning('Error: ' + self.PAIR3 + ' Order doesnt exist')
                            self.exception_logger.warning('Original: ' + str(pair3_order))
                        raise e
                    # if check_count % 4 == 0 and pair3_order['status'] != 'FILLED':
                    #     # reset the price
                    #     pair3_order = update_order(pair3_order, base_quantity[self.COIN1], check_count / 4)
                pair1_executed, pair2_executed, pair3_executed = self.fill_percent(pair1_order, pair2_order, pair3_order)
                if min(pair1_executed, pair2_executed, pair3_executed) > 0.98:
                    break

            if pair1_order != self.EMPTY_ORDER and pair1_order['status'] != 'FILLED':
                print('cancel pair1_order')
                self.cancel_order(pair1_order)
                pair1_order = self.client.get_order(symbol=self.PAIR1,
                                                    orderId=pair1_order['orderId'],
                                                    origClientOrderId=pair1_order['clientOrderId'])
                if pair1_order['status'] == 'CANCELED':
                    pair1_order['memo'] = 'TIMEDOUT'
            if pair2_order != self.EMPTY_ORDER and pair2_order['status'] != 'FILLED':
                print('cancel pair2_order')
                self.cancel_order(pair2_order)
                pair2_order = self.client.get_order(symbol=self.PAIR2,
                                                    orderId=pair2_order['orderId'],
                                                    origClientOrderId=pair2_order['clientOrderId'])
                if pair2_order['status'] == 'CANCELED':
                    pair2_order['memo'] = 'TIMEDOUT'
            if pair3_order != self.EMPTY_ORDER and pair3_order['status'] != 'FILLED':
                print('cancel pair3_order')
                self.cancel_order(pair3_order)
                pair3_order = self.client.get_order(symbol=self.PAIR3,
                                                    orderId=pair3_order['orderId'],
                                                    origClientOrderId=pair3_order['clientOrderId'])
                if pair3_order['status'] == 'CANCELED':
                    pair3_order['memo'] = 'TIMEDOUT'
            # give the system 1 second for balances to be updated
            all_transactions.append(pair1_order)
            all_transactions.append(pair2_order)
            all_transactions.append(pair3_order)

            # see if a market order would still be profitable
            pair1_executed, pair2_executed, pair3_executed = self.fill_percent(pair1_order, pair2_order, pair3_order)
            print(self.PAIR1, pair1_executed, self.PAIR2, pair2_executed, self.PAIR3, pair3_executed)
            total_executed = pair1_executed + pair2_executed + pair3_executed

            pair1_top_off = False
            pair2_top_off = False
            pair3_top_off = False
            if pair1_executed < 0.85 and total_executed > 1.5:
                try:
                    raw_coin1_per_coin3, raw_coin3_per_coin1 = self.calculate_raw_coin_ratio(self.COIN1, self.COIN3)

                    if direction == FORWARD:
                        new_forward_arbitrage = raw_coin1_per_coin3 * coin_per_coin[self.COIN3][self.COIN2] * coin_per_coin[self.COIN2][self.COIN1]
                        # new_forward_arbitrage = raw_coin1_per_coin3 * coin3_per_coin2 * coin2_per_coin1
                        print('new pair1 forward arb: ', '%.3f' % ((new_forward_arbitrage-1.0)*100))
                        if new_forward_arbitrage > self.TOPOFF_THRESHOLD:
                            print("******Topoff opportunity for pair 1*****", )
                            new_quantity = adjusted_quantity[self.COIN3] * (1 - pair1_executed)
                            updated_pair1_order = self.market_convert_coins(self.COIN3, self.COIN1, new_quantity)
                            updated_pair1_order['memo'] = 'TOPOFF'
                            all_transactions.append(updated_pair1_order)
                            pair1_top_off = True
                    else:
                        new_reverse_arbitrage = coin_per_coin[self.COIN1][self.COIN2] * coin_per_coin[self.COIN2][self.COIN3] * raw_coin3_per_coin1
                        # new_reverse_arbitrage = coin1_per_coin2 * coin2_per_coin3 * raw_coin3_per_coin1
                        print('new pair1 reverse arb: ', '%.3f' % ((new_reverse_arbitrage-1.0)*100))
                        if new_reverse_arbitrage > self.TOPOFF_THRESHOLD:
                            print("******Topoff opportunity for pair 1*****")
                            new_quantity = adjusted_quantity[self.COIN1] * (1 - pair1_executed)
                            updated_pair1_order = self.market_convert_coins(self.COIN1, self.COIN3, new_quantity)
                            updated_pair1_order['memo'] = 'TOPOFF'
                            all_transactions.append(updated_pair1_order)
                            pair1_top_off = True
                except Exception as e:
                    print(traceback.format_exc())
            if pair2_executed < 0.85 and total_executed > 1.5:
                try:
                    raw_coin1_per_coin2, raw_coin2_per_coin1 = self.calculate_raw_coin_ratio(self.COIN1, self.COIN2)

                    if direction == FORWARD:
                        new_forward_arbitrage = coin_per_coin[self.COIN1][self.COIN3] * coin_per_coin[self.COIN3][self.COIN2] * raw_coin2_per_coin1
                        # new_forward_arbitrage = coin1_per_coin3 * coin3_per_coin2 * raw_coin2_per_coin1
                        print('new pair2 forward arb: ', '%.3f' % ((new_forward_arbitrage-1.0)*100))
                        if new_forward_arbitrage > self.TOPOFF_THRESHOLD:
                            print("******Topoff opportunity for pair 2*****")
                            new_quantity = adjusted_quantity[self.COIN1] * (1 - pair2_executed)
                            updated_pair2_order = self.market_convert_coins(self.COIN1, self.COIN2, new_quantity)
                            updated_pair2_order['memo'] = 'TOPOFF'
                            all_transactions.append(updated_pair2_order)
                            pair2_top_off = True
                    else:
                        new_reverse_arbitrage = raw_coin1_per_coin2 * coin_per_coin[self.COIN2][self.COIN3] * coin_per_coin[self.COIN3][self.COIN1]
                        # new_reverse_arbitrage = raw_coin1_per_coin2 * coin2_per_coin3 * coin3_per_coin1
                        print('new pair2 reverse arb: ', '%.3f' % ((new_reverse_arbitrage-1.0)*100))
                        if new_reverse_arbitrage > self.TOPOFF_THRESHOLD:
                            print("******Topoff opportunity for pair 2*****")
                            new_quantity = adjusted_quantity[self.COIN2] * (1 - pair2_executed)
                            updated_pair2_order = self.market_convert_coins(self.COIN2, self.COIN1, new_quantity)
                            updated_pair2_order['memo'] = 'TOPOFF'
                            all_transactions.append(updated_pair2_order)
                            pair2_top_off = True
                except Exception as e:
                    print(traceback.format_exc())
            if pair3_executed < 0.85 and total_executed > 1.5:
                try:
                    raw_coin2_per_coin3, raw_coin3_per_coin2 = self.calculate_raw_coin_ratio(self.COIN2, self.COIN3)

                    if direction == FORWARD:
                        new_forward_arbitrage = coin_per_coin[self.COIN1][self.COIN3] * raw_coin3_per_coin2 * coin_per_coin[self.COIN2][self.COIN1]
                        # new_forward_arbitrage = coin1_per_coin3 * raw_coin3_per_coin2 * coin2_per_coin1
                        print('new pair3 forward arb: ', '%.3f' % ((new_forward_arbitrage-1.0)*100))
                        if new_forward_arbitrage > self.TOPOFF_THRESHOLD:
                            print("******Topoff opportunity for pair 3*****")
                            new_quantity = adjusted_quantity[self.COIN2] * (1 - pair3_executed)
                            updated_pair3_order = self.market_convert_coins(self.COIN2, self.COIN3, new_quantity)
                            updated_pair3_order['memo'] = 'TOPOFF'
                            all_transactions.append(updated_pair3_order)
                            pair3_top_off = True
                    else:
                        new_reverse_arbitrage = coin_per_coin[self.COIN1][self.COIN2] * raw_coin2_per_coin3 * coin_per_coin[self.COIN3][self.COIN1]
                        # new_reverse_arbitrage = coin1_per_coin2 * raw_coin2_per_coin3 * coin3_per_coin1
                        print('new pair3 reverse arb: ', '%.3f' % ((new_reverse_arbitrage-1.0)*100))
                        if new_reverse_arbitrage > self.TOPOFF_THRESHOLD:
                            print("******Topoff opportunity for pair 3*****")
                            new_quantity = adjusted_quantity[self.COIN3] * (1 - pair3_executed)
                            updated_pair3_order = self.market_convert_coins(self.COIN3, self.COIN2, new_quantity)
                            updated_pair3_order['memo'] = 'TOPOFF'
                            all_transactions.append(updated_pair3_order)
                            pair3_top_off = True
                except Exception as e:
                    print(traceback.format_exc())

        order_end_time = datetime.utcnow()

        self.update_transaction_log(all_transactions)

        end_coin1_balance = 0.0
        end_coin3_balance = 0.0
        end_coin2_balance = 0.0
        end_bnb_balance = 0.0
        if found_order and (self.balance_book['timestamp'] < order_timestamp or self.balance_book['locked']):
            # the balance book hasn't been updated yet.  This is a minor problem
            # if we had actual orders (it should be logged).
            if (pair1_order is not None and pair1_order != self.EMPTY_ORDER) or \
                    (pair2_order is not None and pair2_order != self.EMPTY_ORDER) or \
                    (pair3_order is not None and pair3_order != self.EMPTY_ORDER):
                self.exception_logger.warning('Time: ' + datetime.utcnow().isoformat())
                self.exception_logger.warning('Warning: Balance book was not updated')
                self.exception_logger.warning('Last updated at: ' + self.balance_book['timestamp'].isoformat())
                self.exception_logger.warning('Order timestamp: ' + order_timestamp.isoformat())
            assets_locked = True
            while assets_locked:
                assets_locked = False
                self.query_coin_balances()
                for coin in self.balance_book:
                    if coin in [self.COIN1, self.COIN2, self.COIN3, 'BNB'] \
                            and self.balance_book[coin]['locked'] > 0:
                        print('Waiting for ' + coin + ' to be unlocked...')
                        # wait till all assets are freed.  sometimes binance can be slow.
                        self.exception_logger.warning('Time: ' + datetime.utcnow().isoformat())
                        self.exception_logger.warning('Warning: Assets still locked')
                        self.exception_logger.warning('Coin: ' + coin + ' Amount: ' + self.balance_book[coin]['locked'])
                        assets_locked = True
                        time.sleep(5.0)
                        continue

        for coin in self.balance_book:
            if coin == self.COIN1:
                end_coin1_balance = self.balance_book[coin]['free']
            elif coin == self.COIN2:
                end_coin2_balance = self.balance_book[coin]['free']
            elif coin == self.COIN3:
                end_coin3_balance = self.balance_book[coin]['free']
            elif coin == 'BNB':
                end_bnb_balance = self.balance_book[coin]['free']
        end_coin1_value = end_coin1_balance * price[self.COIN1]
        end_coin2_value = end_coin2_balance * price[self.COIN2]
        end_coin3_value = end_coin3_balance * price[self.COIN3]
        end_bnb_value = end_bnb_balance * price['BNB']

        pair1_filled = float(pair1_order['executedQty']) + float(updated_pair1_order['executedQty'])
        pair2_filled = float(pair2_order['executedQty']) + float(updated_pair2_order['executedQty'])
        pair3_filled = float(pair3_order['executedQty']) + float(updated_pair3_order['executedQty'])
        if pair1_order != self.EMPTY_ORDER:
            pair1_filled /= float(pair1_order['origQty'])
        if pair2_order != self.EMPTY_ORDER:
            pair2_filled /= float(pair2_order['origQty'])
        if pair3_order != self.EMPTY_ORDER:
            pair3_filled /= float(pair3_order['origQty'])

        start_total_value = start_value[self.COIN1]+start_value[self.COIN2]+start_value[self.COIN3]+start_value['BNB']
        end_total_value = end_coin1_value+end_coin2_value+end_coin3_value+end_bnb_value
        if pair1_filled + pair2_filled + pair3_filled > 2.5:
            final_return = end_total_value-start_total_value
            self.total_return += final_return
            final_return = '%.4f' % final_return
        else:
            final_return = '-'
        start_total_coin1_balance = start_balance[self.COIN1] + (start_balance[self.COIN2] * coin_per_coin[self.COIN1][self.COIN2]) + (start_balance[self.COIN3] * coin_per_coin[self.COIN1][self.COIN3])
        start_total_coin2_balance = (start_balance[self.COIN1] * coin_per_coin[self.COIN2][self.COIN1]) + start_balance[self.COIN2] + (start_balance[self.COIN3] * coin_per_coin[self.COIN2][self.COIN3])
        start_total_coin3_balance = (start_balance[self.COIN1] * coin_per_coin[self.COIN3][self.COIN1]) + (start_balance[self.COIN2] * coin_per_coin[self.COIN3][self.COIN2]) + start_balance[self.COIN3]
        end_total_coin1_balance = end_coin1_balance + (end_coin2_balance * coin_per_coin[self.COIN1][self.COIN2]) + (end_coin3_balance * coin_per_coin[self.COIN1][self.COIN3])
        end_total_coin2_balance = (end_coin1_balance * coin_per_coin[self.COIN2][self.COIN1]) + end_coin2_balance + (end_coin3_balance * coin_per_coin[self.COIN2][self.COIN3])
        end_total_coin3_balance = (end_coin1_balance * coin_per_coin[self.COIN3][self.COIN1]) + (end_coin2_balance * coin_per_coin[self.COIN3][self.COIN2]) + end_coin3_balance

        # start_total_coin1_balance = start_balance[self.COIN1] + (start_balance[self.COIN2] * coin1_per_coin2) + (start_balance[self.COIN3] * coin1_per_coin3)
        # start_total_coin2_balance = (start_balance[self.COIN1] * coin2_per_coin1) + start_balance[self.COIN2] + (start_balance[self.COIN3] * coin2_per_coin3)
        # start_total_coin3_balance = (start_balance[self.COIN1] * coin3_per_coin1) + (start_balance[self.COIN2] * coin3_per_coin2) + start_balance[self.COIN3]
        # end_total_coin1_balance = end_coin1_balance + (end_coin2_balance * coin1_per_coin2) + (end_coin3_balance * coin1_per_coin3)
        # end_total_coin2_balance = (end_coin1_balance * coin2_per_coin1) + end_coin2_balance + (end_coin3_balance * coin2_per_coin3)
        # end_total_coin3_balance = (end_coin1_balance * coin3_per_coin1) + (end_coin2_balance * coin3_per_coin2) + end_coin3_balance

        if found_order:
            print(self.COIN1 + ' ending diff:', end_coin1_balance - start_balance[self.COIN1])
            print(self.COIN2 + ' ending diff:', end_coin2_balance - start_balance[self.COIN2])
            print(self.COIN3 + ' ending diff:', end_coin3_balance - start_balance[self.COIN3])
            print('BNB ending diff:', end_bnb_balance - start_balance['BNB'])

            log_list = ['binance', 'v1.4', order_start_time.isoformat(), order_end_time.isoformat(),
                        '%.4f' % (order_end_time.timestamp()-order_start_time.timestamp()),
                        self.PAIR1, self.PAIR2, self.PAIR3,
                        final_return, '%.4f' % start_total_value, '%.4f' % end_total_value,
                        original_raw_order_book[self.PAIR1].bid, original_raw_order_book[self.PAIR1].ask,
                        original_raw_order_book[self.PAIR2].bid, original_raw_order_book[self.PAIR2].ask,
                        original_raw_order_book[self.PAIR3].bid, original_raw_order_book[self.PAIR3].ask,
                        self.FEE, self.THRESHOLD, self.TOPOFF_THRESHOLD,
                        forward_arbitrage, reverse_arbitrage,
                        adjusted_quantity[self.COIN1], adjusted_quantity[self.COIN2], adjusted_quantity[self.COIN3],
                        pair1_order['status']=='FILLED', pair2_order['status']=='FILLED', pair3_order['status']=='FILLED',
                        start_balance[self.COIN1], end_coin1_balance, start_value[self.COIN1], end_coin1_value,
                        start_balance[self.COIN2], end_coin2_balance, start_value[self.COIN2], end_coin2_value,
                        start_balance[self.COIN3], end_coin3_balance, start_value[self.COIN3], end_coin3_value,
                        start_balance['BNB'], end_bnb_balance, end_bnb_balance - start_balance['BNB'],
                        start_value['BNB'], end_bnb_value, end_bnb_value - start_value['BNB'],
                        delta[self.COIN1], delta[self.COIN2], delta[self.COIN3],
                        start_total_coin1_balance, end_total_coin1_balance,
                        start_total_coin2_balance, end_total_coin2_balance,
                        start_total_coin3_balance, end_total_coin3_balance,
                        pair1_filled, pair2_filled, pair3_filled,
                        pair1_top_off, pair2_top_off, pair3_top_off]
            log_string = ','.join(str(x) for x in log_list)
            print('log line: ', log_string)
            self.order_logger.info(log_string)

        # print(COIN1 + ' gain: ', end_coin1_value - start_value[self.COIN1])
        # print(COIN2 + ' gain: ', end_coin2_value - start_value[self.COIN2])
        # print(COIN3 + ' gain: ', end_coin3_value - start_value[self.COIN3])
        print('total start: ', start_total_value, 'total end: ', end_total_value)
        print('total ' + self.COIN1 + ': ', end_total_coin1_balance, 'total ' + self.COIN2 + ': ', end_total_coin2_balance, 'total ' + self.COIN3 + ': ', end_total_coin3_balance)
        print('return: ', final_return, self.COIN1, (end_coin1_balance-start_balance[self.COIN1]), self.total_return)

        # Make sure we don't drop in value too much
        # TODO: This isn't working properly.  Account isn't being updated?
        # self.all_time_high = max(self.all_time_high, end_total_value)
        # if end_total_value < 0.60 * self.all_time_high and final_return < 0.0:
        #     self.exception_logger.error('Time: ' + datetime.utcnow().isoformat())
        #     self.exception_logger.error('Total value has dropped below 60% of all time high and lost on a trade')
        #     self.exception_logger.error('All Time High: ' + str(self.all_time_high) + 'Current Value: ' + str(end_total_value))
        #     print('Total value has dropped below 60% of all time high and lost on a trade')
        #     print('All Time High: ', self.all_time_high, 'Current Value: ', end_total_value)
            # sys.exit(-1)

        if found_order:
            # pause a short bit so we can read the results
            # long term, this can be removed
            time.sleep(1)
        time.sleep(1.1)


    def check_sockets(self):
        # restart all sockets if they've been up more than half a day
        current_time = datetime.utcnow()
        if current_time > self.socket_start_time + HALF_DAY:
            self.shutdown_socket_listeners()
            self.launch_socket_listeners()


    def check_logs(self):
        # restart all sockets if they've been up more than half a day
        current_time = datetime.utcnow().date()
        if current_time >= self.log_start_time + ONE_DAY:
            # starting the loggers will close down the old ones.
            self.start_logging()



    def run_arbitrage(self):
        global client
        global bm

        self.start_logging()
        self.cancel_all_orders()
        self.query_coin_balances()
        self.launch_socket_listeners()

        exception_count = 0
        while True:
            try:
                self.check_arbitrage()
                self.check_sockets()
                self.check_logs()
                exception_count = 0
            except exceptions.BinanceAPIException as e:
                self.exception_logger.error('Time: ' + datetime.utcnow().isoformat())
                self.exception_logger.error(traceback.format_exc())
                if e.code == -1021:
                    self.exception_logger.info('Timestamp error, pausing and trying again')
                    print('Timestamp error code: ', e)
                    print('Pausing and trying again')
                    exception_count += 1
                    if exception_count >= 3:
                        # this exception keeps showing up so something must be wrong.  cancel
                        # all orders and re-raise the exception
                        self.cancel_all_orders()
                        # raise e
                    time.sleep(3)
                elif e.code == -1001:
                    self.exception_logger.error('Disconnect error, pausing and reconnecting')
                    print('Disconnected, pause and reconnect', e)
                    exception_count += 1
                    if exception_count >= 3:
                        # too many exceptions are occurring so something must be wrong.  shutdown
                        # everything.
                        self.cancel_all_orders()
                        # raise e
                    self.shutdown_socket_listeners()
                    time.sleep(3)
                    self.launch_socket_listeners()
                elif e.code == -2010:
                    # insufficient funds.  this should never happen if we have accurate
                    # values for our coin balances.  try restarting just about everything
                    self.exception_logger.error('Time: ' + datetime.utcnow().isoformat())
                    self.exception_logger.error('Exception placing an order, insufficient funds')
                    self.exception_logger.error(self.COIN1 + ' Funds: ' + str(self.balance_book[self.COIN1]['free'])
                                                + ' ' + str(self.balance_book[self.COIN1]['locked']))
                    self.exception_logger.error(self.COIN2 + ' Funds: ' + str(self.balance_book[self.COIN2]['free'])
                                                + ' ' + str(self.balance_book[self.COIN1]['locked']))
                    self.exception_logger.error(self.COIN3 + ' Funds: ' + str(self.balance_book[self.COIN3]['free'])
                                                + ' ' + str(self.balance_book[self.COIN1]['locked']))
                    self.exception_logger.error(traceback.format_exc())
                    print('Exception placing order', e)
                    exception_count += 1
                    # if exception_count >= 5:
                    #     # too many exceptions are occurring so something must be wrong.  shutdown
                    #     # everything.
                    #     self.cancel_all_orders()
                    #     raise e
                    self.cancel_all_orders()
                    self.shutdown_socket_listeners()
                    self.launch_socket_listeners()
                    self.query_coin_balances()
                else:
                    raise e
            except requests.exceptions.ReadTimeout as e:
                self.exception_logger.error('Time: ' + datetime.utcnow().isoformat())
                self.exception_logger.error('Disconnect error, pausing and reconnecting')
                self.exception_logger.error(traceback.format_exc())
                print('Disconnected, pause and reconnect', e)
                exception_count += 1
                # if exception_count >= 3:
                #     # too many exceptions are occurring so something must be wrong.  shutdown
                #     # everything.
                #     raise e
                time.sleep(3)
                self.client = Client(self.api_key, self.api_secret)
                self.bm = BinanceSocketManager(self.client)
                self.cancel_all_orders()
                self.query_coin_balances()
                self.launch_socket_listeners()
            except Exception as e:
                print('Exitting on exception: ', e)
                self.exception_logger.error('Time: ' + datetime.utcnow().isoformat())
                self.exception_logger.error(traceback.format_exc())
                self.shutdown_socket_listeners()
                # raise e
                time.sleep(3)
                self.client = Client(self.api_key, self.api_secret)
                self.bm = BinanceSocketManager(self.client)
                self.cancel_all_orders()
                self.query_coin_balances()
                self.launch_socket_listeners()


if __name__ == "__main__":
    exception_count = 0
    while True:
        try:
            start_time = datetime.utcnow()
            binance_arbitrage = BinanceArbitrage()
            binance_arbitrage.run_arbitrage()
        except Exception as e:
            print('Failure at the top level', e)
            exception_time = datetime.utcnow()
            if exception_time - start_time < timedelta(minutes=30):
                if exception_count > 3:
                    raise e
                else:
                    exception_count += 1
            else:
                exception_count = 0
            binance_arbitrage = None
            time.sleep(60)
            gc.collect()
            time.sleep(60)


