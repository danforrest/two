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

EMPTY_ORDER = {'status': 'None',
               'orderId': 'None',
               'price': 0.0,
               'origQty': 0.0,
               'executedQty': 0.0,
               'side': 'None'}

class BinanceArbitrage():
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
    neobtc_conn_key = None
    neoeth_conn_key = None
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

    FEE = 0.00015
    THRESHOLD = 1.0009#16 # + (4 * FEE)
    TOPOFF_THRESHOLD = 1.0005
    BNB_QUANTITY = 6.0

    raw_order_book = {'BNBBTC': OrderBook(),
                      'ETHBTC': OrderBook(),
                      'BNBETH': OrderBook(),
                      'NEOBTC': OrderBook(),
                      'NEOETH': OrderBook(),
                      'BTCUSDT': OrderBook(),
                      'ETHUSDT': OrderBook(),
                      'BNBUSDT': OrderBook()}

    balance_book = {'timestamp': None,
                    'BNB': 0.0,
                    'ETH': 0.0,
                    'BTC': 0.0,
                    'NEO': 0.0,
                    'USDT': 0.0}

    trade_order_book = {}

    total_return = 0.0
    all_time_high = 0.0


    def process_bnbbtc_depth_message(self, msg):
        self.raw_order_book['BNBBTC'].bid = float(msg['bids'][0][0])
        self.raw_order_book['BNBBTC'].ask = float(msg['asks'][0][0])


    def process_ethbtc_depth_message(self, msg):
        self.raw_order_book['ETHBTC'].bid = float(msg['bids'][0][0])
        self.raw_order_book['ETHBTC'].ask = float(msg['asks'][0][0])


    def process_bnbeth_depth_message(self, msg):
        self.raw_order_book['BNBETH'].bid = float(msg['bids'][0][0])
        self.raw_order_book['BNBETH'].ask = float(msg['asks'][0][0])


    def process_neobtc_depth_message(self, msg):
        self.raw_order_book['NEOBTC'].bid = float(msg['bids'][0][0])
        self.raw_order_book['NEOBTC'].ask = float(msg['asks'][0][0])


    def process_neoeth_depth_message(self, msg):
        self.raw_order_book['NEOETH'].bid = float(msg['bids'][0][0])
        self.raw_order_book['NEOETH'].ask = float(msg['asks'][0][0])


    def process_btcusdt_depth_message(self, msg):
        self.raw_order_book['BTCUSDT'].bid = float(msg['bids'][0][0])
        self.raw_order_book['BTCUSDT'].ask = float(msg['asks'][0][0])


    def process_ethusdt_depth_message(self, msg):
        self.raw_order_book['ETHUSDT'].bid = float(msg['bids'][0][0])
        self.raw_order_book['ETHUSDT'].ask = float(msg['asks'][0][0])


    def process_bnbusdt_depth_message(self, msg):
        self.raw_order_book['BNBUSDT'].bid = float(msg['bids'][0][0])
        self.raw_order_book['BNBUSDT'].ask = float(msg['asks'][0][0])


    def process_account_message(self, msg):
        #print('user stream message: ', msg)
        if 'e' in msg and msg['e'] != 'outboundAccountInfo':
            # we only care about account info for now
            return
        if 'B' not in msg:
            # the outboundAccountInfo message should have balances
            return
        for asset in msg['B']:
            if asset['a'] in self.balance_book:
                #print('asset: ', asset['a'], 'balance: ', asset['f'])
                self.balance_book[asset['a']] = float(asset['f']) + float(asset['l'])
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
        # new_quantity = round(float(order['origQty']) - float(order['executedQty']), 2)
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
        # try:
        #     if order['side'] == 'BUY':
        #         client.order_market_buy(symbol=order['symbol'], quantity=new_quantity)
        #     else:
        #         client.order_market_sell(symbol=order['symbol'], quantity=new_quantity)
        # except Exception as e:
        #     print('Exception while attempting to cancel order: ', order)
        #     print(e)


    @staticmethod
    def quick_calc(a_quantity, b_per_a, c_per_b, a_per_c):
        b_quantity = b_per_a * a_quantity
        c_quantity = c_per_b * b_quantity
        a_result = a_per_c * c_quantity

        return b_quantity, c_quantity, a_result


    def build_trade_order_book(self):
        pair1_spread = round((self.raw_order_book[self.PAIR1].ask - self.raw_order_book[self.PAIR1].bid) / self.TICK[self.PAIR1], 0)
        pair2_spread = round((self.raw_order_book[self.PAIR2].ask - self.raw_order_book[self.PAIR2].bid) / self.TICK[self.PAIR2], 0)
        pair3_spread = round((self.raw_order_book[self.PAIR3].ask - self.raw_order_book[self.PAIR3].bid) / self.TICK[self.PAIR3], 0)
        print(self.PAIR1 + ' spread: ', pair1_spread)
        print(self.PAIR2 + ' spread: ', pair2_spread)
        print(self.PAIR3 + ' spread: ', pair3_spread)

        # pick a price in the middle of the spread and see if that works for arbitrage
        trade_order = {self.PAIR1: OrderBook(),
                       self.PAIR2: OrderBook(),
                       self.PAIR3: OrderBook()}
        trade_order[self.PAIR1].bid = max(self.raw_order_book[self.PAIR1].bid,
                                          self.raw_order_book[self.PAIR1].ask - (
                                             self.SPREAD_THRESHOLD[self.PAIR1] * pair1_spread * self.TICK[self.PAIR1]))
        trade_order[self.PAIR1].ask = min(self.raw_order_book[self.PAIR1].ask,
                                          self.raw_order_book[self.PAIR1].bid + (
                                             self.SPREAD_THRESHOLD[self.PAIR1] * pair1_spread * self.TICK[self.PAIR1]))
        trade_order[self.PAIR2].bid = max(self.raw_order_book[self.PAIR2].bid,
                                          self.raw_order_book[self.PAIR2].ask - (
                                             self.SPREAD_THRESHOLD[self.PAIR2] * pair2_spread * self.TICK[self.PAIR2]))
        trade_order[self.PAIR2].ask = min(self.raw_order_book[self.PAIR2].ask,
                                          self.raw_order_book[self.PAIR2].bid + (
                                             self.SPREAD_THRESHOLD[self.PAIR2] * pair2_spread * self.TICK[self.PAIR2]))
        trade_order[self.PAIR3].bid = max(self.raw_order_book[self.PAIR3].bid,
                                          self.raw_order_book[self.PAIR3].ask - (
                                             self.SPREAD_THRESHOLD[self.PAIR3] * pair3_spread * self.TICK[self.PAIR3]))
        trade_order[self.PAIR3].ask = min(self.raw_order_book[self.PAIR3].ask,
                                          self.raw_order_book[self.PAIR3].bid + (
                                             self.SPREAD_THRESHOLD[self.PAIR3] * pair3_spread * self.TICK[self.PAIR3]))

        trade_order[self.PAIR1].bid = round(trade_order[self.PAIR1].bid, self.PRICE_PRECISION[self.PAIR1])
        trade_order[self.PAIR1].ask = round(trade_order[self.PAIR1].ask, self.PRICE_PRECISION[self.PAIR1])
        trade_order[self.PAIR2].bid = round(trade_order[self.PAIR2].bid, self.PRICE_PRECISION[self.PAIR2])
        trade_order[self.PAIR2].ask = round(trade_order[self.PAIR2].ask, self.PRICE_PRECISION[self.PAIR2])
        trade_order[self.PAIR3].bid = round(trade_order[self.PAIR3].bid, self.PRICE_PRECISION[self.PAIR3])
        trade_order[self.PAIR3].ask = round(trade_order[self.PAIR3].ask, self.PRICE_PRECISION[self.PAIR3])

        print(self.PAIR1 + ' bid: ', self.PRICE_FORMAT[self.PAIR1] % trade_order[self.PAIR1].bid,
              self.PAIR1 + ' ask: ', self.PRICE_FORMAT[self.PAIR1] % trade_order[self.PAIR1].ask)
        print(self.PAIR2 + ' bid: ', self.PRICE_FORMAT[self.PAIR2] % trade_order[self.PAIR2].bid,
              self.PAIR2 + ' ask: ', self.PRICE_FORMAT[self.PAIR2] % trade_order[self.PAIR2].ask)
        print(self.PAIR3 + ' bid: ', self.PRICE_FORMAT[self.PAIR3] % trade_order[self.PAIR3].bid,
              self.PAIR3 + ' ask: ', self.PRICE_FORMAT[self.PAIR3] % trade_order[self.PAIR3].ask)

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


    # def calculate_coin_ratio(self, coin1, coin2):
    #     if coin1+coin2 in [self.PAIR1, self.PAIR2, self.PAIR3]:
    #         coin1_per_coin2 = 1 / self.trade_order_book[coin1+coin2].bid
    #         coin2_per_coin1 = self.trade_order_book[coin1+coin2].ask
    #     elif coin2+coin1 in [self.PAIR1, self.PAIR2, self.PAIR3]:
    #         coin2_per_coin1 = 1 / self.trade_order_book[coin2+coin1].bid
    #         coin1_per_coin2 = self.trade_order_book[coin2+coin1].ask
    #     else:
    #         error_string = 'No pairs found for coins', coin1, coin2, 'in: ', self.PAIR1, self.PAIR2, self.PAIR3
    #         print(error_string)
    #         raise Exception(error_string)
    #
    #     return coin1_per_coin2, coin2_per_coin1


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
            # print('SELL: ', pair, 'price: ', price, 'quantity: ', adjusted_quantity)
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
                #order = None
        elif coin2+coin1 in [self.PAIR1, self.PAIR2, self.PAIR3]:
            # buy
            pair = coin2+coin1
            price = self.PRICE_FORMAT[pair] % order_book[pair].bid
            coin1_per_coin2, coin2_per_coin1 = self.calculate_coin_ratio(coin1, coin2, order_book)
            adjusted_quantity = round(quantity * coin2_per_coin1, self.QUANTITY_PRECISION[pair])
            # convert coin1 quantity to coin2 quantity
            # print('BUY: ', pair, 'price: ', price, 'quantity: ', adjusted_quantity)
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
                #order = None
        else:
            order = None

        # print('Order: ', order)
        return order


    def market_convert_coins(self, coin1, coin2, quantity):
        # TODO: Need min amount in addition to min notional
        if coin1+coin2 in [self.PAIR1, self.PAIR2, self.PAIR3]:
            # sell
            pair = coin1+coin2
            price = self.PRICE_FORMAT[pair] % self.raw_order_book[pair].ask
            adjusted_quantity = round(quantity, self.QUANTITY_PRECISION[pair])
            # print('SELL: ', pair, 'price: ', price, 'quantity: ', adjusted_quantity)
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
                #order = None
        elif coin2+coin1 in [self.PAIR1, self.PAIR2, self.PAIR3]:
            # buy
            pair = coin2+coin1
            price = self.PRICE_FORMAT[pair] % self.raw_order_book[pair].bid
            coin1_per_coin2, coin2_per_coin1 = self.calculate_raw_coin_ratio(coin1, coin2)
            adjusted_quantity = round(quantity * coin2_per_coin1, self.QUANTITY_PRECISION[pair])
            # convert coin1 quantity to coin2 quantity
            # print('BUY: ', pair, 'price: ', price, 'quantity: ', adjusted_quantity)
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
                #order = None
        else:
            order = None

        # print('Order: ', order)
        return order


    @staticmethod
    def print_order_status(pair1_order, pair2_order, pair3_order):
        status_string = 'Status:  '
        if pair1_order is None or pair1_order['status'] == 'None':
            status_string += '---  '
        else:
            status_string += '{:3d}  '.format(int(100*float(pair1_order['executedQty'])/float(pair1_order['origQty'])))
        if pair2_order is None or pair2_order['status'] == 'None':
            status_string += '---  '
        else:
            status_string += '{:3d}  '.format(int(100*float(pair2_order['executedQty'])/float(pair2_order['origQty'])))
        if pair3_order is None or pair3_order['status'] == 'None':
            status_string += '---  '
        else:
            status_string += '{:3d}  '.format(int(100*float(pair3_order['executedQty'])/float(pair3_order['origQty'])))

        print(status_string)


    def query_coin_balances(self):
        result = self.client.get_account()
        self.balance_book[self.COIN1] = 0.0
        self.balance_book[self.COIN2] = 0.0
        self.balance_book[self.COIN3] = 0.0
        self.balance_book['BNB'] = 0.0
        for asset in result['balances']:
            if asset['asset'] == self.COIN1:
                self.balance_book[asset['asset']] += float(asset['free'])
            elif asset['asset'] == self.COIN2:
                self.balance_book[asset['asset']] += float(asset['free'])
            elif asset['asset'] == self.COIN3:
                self.balance_book[asset['asset']] += float(asset['free'])
            elif asset['asset'] == 'BNB':
                self.balance_book[asset['asset']] += float(asset['free'])
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
        self.bm = BinanceSocketManager(self.client)
        # start any sockets here, i.e a trade socket
        self.bnbbtc_conn_key = self.bm.start_depth_socket('BNBBTC',
                                                          self.process_bnbbtc_depth_message,
                                                          depth=BinanceSocketManager.WEBSOCKET_DEPTH_5)
        self.ethbtc_conn_key = self.bm.start_depth_socket('ETHBTC',
                                                          self.process_ethbtc_depth_message,
                                                          depth=BinanceSocketManager.WEBSOCKET_DEPTH_5)
        self.bnbeth_conn_key = self.bm.start_depth_socket('BNBETH',
                                                          self.process_bnbeth_depth_message,
                                                          depth=BinanceSocketManager.WEBSOCKET_DEPTH_5)
        self.neobtc_conn_key = self.bm.start_depth_socket('NEOBTC',
                                                          self.process_neobtc_depth_message,
                                                          depth=BinanceSocketManager.WEBSOCKET_DEPTH_5)
        self.neoeth_conn_key = self.bm.start_depth_socket('NEOETH',
                                                          self.process_neoeth_depth_message,
                                                          depth=BinanceSocketManager.WEBSOCKET_DEPTH_5)
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
        while self.raw_order_book['BNBETH'].bid == 0 or \
                self.raw_order_book['ETHBTC'].bid == 0 or \
                self.raw_order_book['BNBBTC'].bid == 0 or \
                self.raw_order_book['NEOBTC'].bid == 0 or \
                self.raw_order_book['NEOETH'].bid == 0 or \
                self.raw_order_book['BTCUSDT'].bid == 0 or \
                self.raw_order_book['ETHUSDT'].bid == 0 or \
                self.raw_order_book['BNBUSDT'].bid == 0:
            time.sleep(1)


    def shutdown_socket_listeners(self):
        self.bm.stop_socket(self.bnbbtc_conn_key)
        self.bm.stop_socket(self.ethbtc_conn_key)
        self.bm.stop_socket(self.bnbeth_conn_key)
        self.bm.stop_socket(self.neobtc_conn_key)
        self.bm.stop_socket(self.neoeth_conn_key)
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


    def check_arbitrage(self):
        # calculate balance of each coin
        start_coin1_balance = 0.0
        start_coin2_balance = 0.0
        start_coin3_balance = 0.0
        start_bnb_balance = 0.0
        all_transactions = []
        for coin in self.balance_book:
            if coin == self.COIN1:
                start_coin1_balance = self.balance_book[coin]
            elif coin == self.COIN2:
                start_coin2_balance = self.balance_book[coin]
            elif coin == self.COIN3:
                start_coin3_balance = self.balance_book[coin]
            elif coin == 'BNB':
                start_bnb_balance = self.balance_book[coin]

        # calculate the value of each coin in dollars
        if self.COIN1+'USDT' in self.raw_order_book:
            coin1_price = self.raw_order_book[self.COIN1+'USDT'].ask
        if self.COIN2+'USDT' in self.raw_order_book:
            coin2_price = self.raw_order_book[self.COIN2+'USDT'].ask
        if self.COIN3+'USDT' in self.raw_order_book:
            coin3_price = self.raw_order_book[self.COIN3+'USDT'].ask
        if 'BNBUSDT' in self.raw_order_book:
            bnb_price = self.raw_order_book['BNBUSDT'].ask
        # USDT prices are always 1.0 coin per dollar
        if self.COIN1 == 'USDT':
            coin1_price = 1.0
        elif self.COIN2 == 'USDT':
            coin2_price = 1.0
        elif self.COIN3 == 'USDT':
            coin3_price = 1.0

        start_coin1_value = start_coin1_balance * coin1_price
        start_coin2_value = start_coin2_balance * coin2_price
        start_coin3_value = start_coin3_balance * coin3_price
        start_bnb_value = start_bnb_balance * bnb_price

        print(self.COIN1 + ' starting balance:', start_coin1_balance)
        print(self.COIN2 + ' starting balance:', start_coin2_balance)
        print(self.COIN3 + ' starting balance:', start_coin3_balance)
        print('BNB starting balance:', start_bnb_balance)

        if start_bnb_value < 0.001 * (start_coin1_value+start_coin2_value+start_coin3_value):
            order = self.client.order_market_buy(symbol='BNBUSDT', quantity=2.00, newOrderRespType='FULL')
            # TODO: Log this somehow
            while 'status' not in order or order['status'] != 'FILLED':
                time.sleep(0.5)
                order = self.client.get_order(symbol='BNBUSDT', orderId=order['orderId'])
            order['memo'] = 'FEE_PAYMENT'
            all_transactions.append(order)

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
        average_value = (start_coin1_value + start_coin2_value + start_coin3_value) / 3.0
        delta_coin1 = self.calculate_coin_delta(start_coin1_value, average_value, coin1_price)
        delta_coin2 = self.calculate_coin_delta(start_coin2_value, average_value, coin2_price)
        delta_coin3 = self.calculate_coin_delta(start_coin3_value, average_value, coin3_price)
        print('average value: ', average_value)
        print(self.COIN1 + ' value: ', start_coin1_value)
        print(self.COIN2 + ' value: ', start_coin2_value)
        print(self.COIN3 + ' value: ', start_coin3_value)
        print('delta ' + self.COIN1 + ': ', delta_coin1)
        print('delta ' + self.COIN2 + ': ', delta_coin2)
        print('delta ' + self.COIN3 + ': ', delta_coin3)

        order_start_time = datetime.utcnow().isoformat()

        # print(PAIR1 + ' bid: ', self.raw_order_book[PAIR1].bid, ' ask: ', self.raw_order_book[PAIR1].ask)
        # print(PAIR2 + ' bid: ', self.raw_order_book[PAIR2].bid, ' ask: ', self.raw_order_book[PAIR2].ask)
        # print(PAIR3 + ' bid: ', self.raw_order_book[PAIR3].bid, ' ask: ', self.raw_order_book[PAIR3].ask)

        # pick a price in the middle of the spread and see if that works for arbitrage
        self.trade_order_book = self.build_trade_order_book()
        original_order_book = copy.deepcopy(self.trade_order_book)
        original_raw_order_book = copy.deepcopy(self.raw_order_book)

        print('raw', self.PAIR1 + ' bid: ', self.PRICE_FORMAT[self.PAIR1] % self.raw_order_book[self.PAIR1].bid,
              self.PAIR1 + ' ask: ', self.PRICE_FORMAT[self.PAIR1] % self.raw_order_book[self.PAIR1].ask)
        print('raw', self.PAIR2 + ' bid: ', self.PRICE_FORMAT[self.PAIR2] % self.raw_order_book[self.PAIR2].bid,
              self.PAIR2 + ' ask: ', self.PRICE_FORMAT[self.PAIR2] % self.raw_order_book[self.PAIR2].ask)
        print('raw', self.PAIR3 + ' bid: ', self.PRICE_FORMAT[self.PAIR3] % self.raw_order_book[self.PAIR3].bid,
              self.PAIR3 + ' ask: ', self.PRICE_FORMAT[self.PAIR3] % self.raw_order_book[self.PAIR3].ask)

        coin1_per_coin2, coin2_per_coin1 = self.calculate_coin_ratio(self.COIN1, self.COIN2, original_order_book)
        coin1_per_coin3, coin3_per_coin1 = self.calculate_coin_ratio(self.COIN1, self.COIN3, original_order_book)
        coin2_per_coin3, coin3_per_coin2 = self.calculate_coin_ratio(self.COIN2, self.COIN3, original_order_book)

        # print(self.COIN1 + '_per_' + self.COIN3 + ': ', coin1_per_coin3, coin3_per_coin1)
        # print(self.COIN3 + '_per_' + self.COIN2 + ': ', coin3_per_coin2, coin2_per_coin3)
        # print(self.COIN2 + '_per_' + self.COIN1 + ': ', coin2_per_coin1, coin1_per_coin2)

        forward_arbitrage = coin1_per_coin3 * coin3_per_coin2 * coin2_per_coin1
        reverse_arbitrage = coin1_per_coin2 * coin2_per_coin3 * coin3_per_coin1

        print('forward: ', forward_arbitrage)
        print('reverse: ', reverse_arbitrage)

        if forward_arbitrage > reverse_arbitrage:
            direction = FORWARD
            gain = forward_arbitrage
        else:
            direction = REVERSE
            gain = reverse_arbitrage

        # raw_coin1_per_coin2, raw_coin2_per_coin1 = self.calculate_raw_coin_ratio(self.COIN1, self.COIN2)
        # raw_coin1_per_coin3, raw_coin3_per_coin1 = self.calculate_raw_coin_ratio(self.COIN1, self.COIN3)
        # raw_coin2_per_coin3, raw_coin3_per_coin2 = self.calculate_raw_coin_ratio(self.COIN2, self.COIN3)
        #
        # raw_forward_arbitrage = raw_coin1_per_coin3 * raw_coin3_per_coin2 * raw_coin2_per_coin1
        # raw_reverse_arbitrage = raw_coin1_per_coin2 * raw_coin2_per_coin3 * raw_coin3_per_coin1

        # print('raw forward: ', raw_forward_arbitrage)
        # print('raw reverse: ', raw_reverse_arbitrage)

        coin2_quantity = 0.0
        coin3_quantity = 0.0
        coin1_result = 0.0
        pair3_order = None
        pair2_order = None
        pair1_order = None
        found_order = True

        order_timestamp = datetime.utcnow()
        if direction == FORWARD and gain > self.THRESHOLD:
            print('doing forward arbitrage')
            coin2_quantity, coin3_quantity, coin1_result = self.quick_calc(base_quantity,
                                                                           coin2_per_coin1,
                                                                           coin3_per_coin2,
                                                                           coin1_per_coin3)
            pair1_price = self.PRICE_FORMAT[self.PAIR1] % original_order_book[self.PAIR1].bid
            pair2_price = self.PRICE_FORMAT[self.PAIR2] % original_order_book[self.PAIR2].ask
            pair3_price = self.PRICE_FORMAT[self.PAIR3] % original_order_book[self.PAIR3].ask
            print(self.PAIR1 + ' price: ', pair1_price)
            print(self.PAIR2 + ' price: ', pair2_price)
            print(self.PAIR3 + ' price: ', pair3_price)

            adjusted_coin1_quantity = base_quantity - delta_coin1
            adjusted_coin1_quantity += delta_coin2 * coin1_per_coin2
            adjusted_coin1_quantity = min(0.95*start_coin1_balance, adjusted_coin1_quantity)
            adjusted_coin2_quantity = coin2_quantity - delta_coin2
            adjusted_coin2_quantity += delta_coin3 * coin2_per_coin3
            adjusted_coin2_quantity = min(0.95*start_coin2_balance, adjusted_coin2_quantity)
            adjusted_coin3_quantity = coin3_quantity - delta_coin3
            adjusted_coin3_quantity += delta_coin1 * coin3_per_coin1
            adjusted_coin3_quantity = min(0.95*start_coin3_balance, adjusted_coin3_quantity)

            print(self.COIN1 + ': ', base_quantity, adjusted_coin1_quantity)
            print(self.COIN2 + ': ', coin2_quantity, adjusted_coin2_quantity)
            print(self.COIN3 + ': ', coin3_quantity, adjusted_coin3_quantity)

            if adjusted_coin1_quantity > 0:
                pair2_order = self.convert_coins(self.COIN1, self.COIN2, adjusted_coin1_quantity, original_order_book)
            if adjusted_coin2_quantity > 0:
                pair3_order = self.convert_coins(self.COIN2, self.COIN3, adjusted_coin2_quantity, original_order_book)
            if adjusted_coin3_quantity > 0:
                pair1_order = self.convert_coins(self.COIN3, self.COIN1, adjusted_coin3_quantity, original_order_book)
        elif direction == REVERSE and gain > self.THRESHOLD:
            print('doing reverse arbitrage')
            coin3_quantity, coin2_quantity, coin1_result = self.quick_calc(base_quantity,
                                                                           coin3_per_coin1,
                                                                           coin2_per_coin3,
                                                                           coin1_per_coin2)
            pair1_price = self.PRICE_FORMAT[self.PAIR1] % original_order_book[self.PAIR1].ask
            pair2_price = self.PRICE_FORMAT[self.PAIR2] % original_order_book[self.PAIR2].bid
            pair3_price = self.PRICE_FORMAT[self.PAIR3] % original_order_book[self.PAIR3].bid
            print(self.PAIR1 + ' price: ', pair1_price)
            print(self.PAIR2 + ' price: ', pair2_price)
            print(self.PAIR3 + ' price: ', pair3_price)

            adjusted_coin1_quantity = base_quantity - delta_coin1
            adjusted_coin1_quantity += delta_coin3 * coin1_per_coin3
            adjusted_coin1_quantity = min(start_coin1_balance, adjusted_coin1_quantity)
            adjusted_coin2_quantity = coin2_quantity - delta_coin2
            adjusted_coin2_quantity += delta_coin1 * coin2_per_coin1
            adjusted_coin2_quantity = min(start_coin2_balance, adjusted_coin2_quantity)
            adjusted_coin3_quantity = coin3_quantity - delta_coin3
            adjusted_coin3_quantity += delta_coin2 * coin3_per_coin2
            adjusted_coin3_quantity = min(start_coin3_balance, adjusted_coin3_quantity)

            print(self.COIN1 + ': ', base_quantity, adjusted_coin1_quantity)
            print(self.COIN2 + ': ', coin2_quantity, adjusted_coin2_quantity)
            print(self.COIN3 + ': ', coin3_quantity, adjusted_coin3_quantity)

            if adjusted_coin1_quantity > 0:
                pair1_order = self.convert_coins(self.COIN1, self.COIN3, adjusted_coin1_quantity, original_order_book)
            if adjusted_coin3_quantity > 0:
                pair3_order = self.convert_coins(self.COIN3, self.COIN2, adjusted_coin3_quantity, original_order_book)
            if adjusted_coin2_quantity > 0:
                pair2_order = self.convert_coins(self.COIN2, self.COIN1, adjusted_coin2_quantity, original_order_book)
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
                  and start_time + 35 > time.time():
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
                        # pair1_order = update_order(pair1_order, base_quantity, check_count / 4)
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
                    #     pair2_order = update_order(pair2_order, coin2_quantity, check_count/4)
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
                    #     pair3_order = update_order(pair3_order, base_quantity, check_count / 4)

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
            print(self.PAIR1, pair1_executed, self.PAIR2, pair2_executed, self.PAIR3, pair3_executed)
            total_executed = pair1_executed + pair2_executed + pair3_executed

            pair1_top_off = False
            pair2_top_off = False
            pair3_top_off = False
            if pair1_executed < 0.75 and total_executed > 1.6:
                try:
                    raw_coin1_per_coin3, raw_coin3_per_coin1 = self.calculate_raw_coin_ratio(self.COIN1, self.COIN3)

                    if direction == FORWARD:
                        new_forward_arbitrage = raw_coin1_per_coin3 * coin3_per_coin2 * coin2_per_coin1
                        print('new pair1 forward arb: ', new_forward_arbitrage)
                        if new_forward_arbitrage > self.TOPOFF_THRESHOLD:
                            print("******Topoff opportunity for pair 1*****", )
                            new_quantity = adjusted_coin3_quantity * (1 - pair1_executed)
                            updated_pair1_order = self.market_convert_coins(self.COIN3, self.COIN1, new_quantity)
                            updated_pair1_order['memo'] = 'TOPOFF'
                            all_transactions.append(updated_pair1_order)
                            pair1_top_off = True
                    else:
                        new_reverse_arbitrage = coin1_per_coin2 * coin2_per_coin3 * raw_coin3_per_coin1
                        print('new pair1 reverse arb: ', new_reverse_arbitrage)
                        if new_reverse_arbitrage > self.TOPOFF_THRESHOLD:
                            print("******Topoff opportunity for pair 1*****")
                            new_quantity = adjusted_coin1_quantity * (1 - pair1_executed)
                            updated_pair1_order = self.market_convert_coins(self.COIN1, self.COIN3, new_quantity)
                            updated_pair1_order['memo'] = 'TOPOFF'
                            all_transactions.append(updated_pair1_order)
                            pair1_top_off = True
                except Exception as e:
                    print(traceback.format_exc())
            if pair2_executed < 0.75 and total_executed > 1.6:
                try:
                    raw_coin1_per_coin2, raw_coin2_per_coin1 = self.calculate_raw_coin_ratio(self.COIN1, self.COIN2)

                    if direction == FORWARD:
                        new_forward_arbitrage = coin1_per_coin3 * coin3_per_coin2 * raw_coin2_per_coin1
                        print('new pair2 forward arb: ', new_forward_arbitrage)
                        if new_forward_arbitrage > self.TOPOFF_THRESHOLD:
                            print("******Topoff opportunity for pair 2*****")
                            new_quantity = adjusted_coin1_quantity * (1 - pair2_executed)
                            updated_pair2_order = self.market_convert_coins(self.COIN1, self.COIN2, new_quantity)
                            updated_pair2_order['memo'] = 'TOPOFF'
                            all_transactions.append(updated_pair2_order)
                            pair2_top_off = True
                    else:
                        new_reverse_arbitrage = raw_coin1_per_coin2 * coin2_per_coin3 * coin3_per_coin1
                        print('new pair2 reverse arb: ', new_reverse_arbitrage)
                        if new_reverse_arbitrage > self.TOPOFF_THRESHOLD:
                            print("******Topoff opportunity for pair 2*****")
                            new_quantity = adjusted_coin2_quantity * (1 - pair2_executed)
                            updated_pair2_order = self.market_convert_coins(self.COIN2, self.COIN1, new_quantity)
                            updated_pair2_order['memo'] = 'TOPOFF'
                            all_transactions.append(updated_pair2_order)
                            pair2_top_off = True
                except Exception as e:
                    print(traceback.format_exc())
            if pair3_executed < 0.75 and total_executed > 1.6:
                try:
                    raw_coin2_per_coin3, raw_coin3_per_coin2 = self.calculate_raw_coin_ratio(self.COIN2, self.COIN3)

                    if direction == FORWARD:
                        new_forward_arbitrage = coin1_per_coin3 * raw_coin3_per_coin2 * coin2_per_coin1
                        print('new pair3 forward arb: ', new_forward_arbitrage)
                        if new_forward_arbitrage > self.TOPOFF_THRESHOLD:
                            print("******Topoff opportunity for pair 3*****")
                            new_quantity = adjusted_coin2_quantity * (1 - pair3_executed)
                            updated_pair3_order = self.market_convert_coins(self.COIN2, self.COIN3, new_quantity)
                            updated_pair3_order['memo'] = 'TOPOFF'
                            all_transactions.append(updated_pair3_order)
                            pair3_top_off = True
                    else:
                        new_reverse_arbitrage = coin1_per_coin2 * raw_coin2_per_coin3 * coin3_per_coin1
                        print('new pair3 reverse arb: ', new_reverse_arbitrage)
                        if new_reverse_arbitrage > self.TOPOFF_THRESHOLD:
                            print("******Topoff opportunity for pair 3*****")
                            new_quantity = adjusted_coin3_quantity * (1 - pair3_executed)
                            updated_pair3_order = self.market_convert_coins(self.COIN3, self.COIN2, new_quantity)
                            updated_pair3_order['memo'] = 'TOPOFF'
                            all_transactions.append(updated_pair3_order)
                            pair3_top_off = True
                except Exception as e:
                    print(traceback.format_exc())

            time.sleep(1)

        self.update_transaction_log(all_transactions)

        order_end_time = datetime.utcnow().isoformat()

        end_coin1_balance = 0.0
        end_coin3_balance = 0.0
        end_coin2_balance = 0.0
        end_bnb_balance = 0.0
        if found_order and self.balance_book['timestamp'] < order_timestamp:
            # the balance book hasn't been updated yet.  This is a minor problem
            # if we had actual orders (it should be logged).
            if (pair1_order is not None and pair1_order != self.EMPTY_ORDER) or \
                    (pair2_order is not None and pair2_order != self.EMPTY_ORDER) or \
                    (pair3_order is not None and pair3_order != self.EMPTY_ORDER):
                self.exception_logger.warning('Time: ' + datetime.utcnow().isoformat())
                self.exception_logger.warning('Warning: Balance book was not updated')
                self.exception_logger.warning('Last updated at: ' + self.balance_book['timestamp'].isoformat())
                self.exception_logger.warning('Order timestamp: ' + order_timestamp.isoformat())
            self.query_coin_balances()

        for coin in self.balance_book:
            if coin == self.COIN1:
                end_coin1_balance = self.balance_book[coin]
            elif coin == self.COIN2:
                end_coin2_balance = self.balance_book[coin]
            elif coin == self.COIN3:
                end_coin3_balance = self.balance_book[coin]
            elif coin == 'BNB':
                end_bnb_balance = self.balance_book[coin]
        end_coin1_value = end_coin1_balance * coin1_price
        end_coin2_value = end_coin2_balance * coin2_price
        end_coin3_value = end_coin3_balance * coin3_price
        end_bnb_value = end_bnb_balance * bnb_price

        start_total_value = start_coin1_value+start_coin2_value+start_coin3_value+start_bnb_value
        end_total_value = end_coin1_value+end_coin2_value+end_coin3_value+end_bnb_value
        final_return = end_total_value-start_total_value
        self.total_return += final_return
        start_total_coin1_balance = start_coin1_balance + (start_coin2_balance * coin1_per_coin2) + (start_coin3_balance * coin1_per_coin3)
        start_total_coin2_balance = (start_coin1_balance * coin2_per_coin1) + start_coin2_balance + (start_coin3_balance * coin2_per_coin3)
        start_total_coin3_balance = (start_coin1_balance * coin3_per_coin1) + (start_coin2_balance * coin3_per_coin2) + start_coin3_balance
        end_total_coin1_balance = end_coin1_balance + (end_coin2_balance * coin1_per_coin2) + (end_coin3_balance * coin1_per_coin3)
        end_total_coin2_balance = (end_coin1_balance * coin2_per_coin1) + end_coin2_balance + (end_coin3_balance * coin2_per_coin3)
        end_total_coin3_balance = (end_coin1_balance * coin3_per_coin1) + (end_coin2_balance * coin3_per_coin2) + end_coin3_balance

        if found_order:
            print(self.COIN1 + ' ending diff:', end_coin1_balance - start_coin1_balance)
            print(self.COIN2 + ' ending diff:', end_coin2_balance - start_coin2_balance)
            print(self.COIN3 + ' ending diff:', end_coin3_balance - start_coin3_balance)
            print('BNB ending diff:', end_bnb_balance - start_bnb_balance)

            # TODO: this needs to be updated now that orders/bid/ask/qty can change from
            # their initial values.  (Well, only if I change it back to update orders)
            log_list = ['binance', 'v1.3', order_start_time, order_end_time,
                        self.PAIR1, self.PAIR2, self.PAIR3,
                        final_return, start_total_value, end_total_value,
                        original_raw_order_book[self.PAIR1].bid, original_raw_order_book[self.PAIR1].ask,
                        original_raw_order_book[self.PAIR2].bid, original_raw_order_book[self.PAIR2].ask,
                        original_raw_order_book[self.PAIR3].bid, original_raw_order_book[self.PAIR3].ask,
                        self.FEE, self.THRESHOLD, self.TOPOFF_THRESHOLD,
                        forward_arbitrage, reverse_arbitrage,
                        adjusted_coin1_quantity, adjusted_coin3_quantity, adjusted_coin3_quantity,
                        pair1_order!=self.EMPTY_ORDER, pair2_order!=self.EMPTY_ORDER, pair3_order!=self.EMPTY_ORDER,
                        start_coin1_balance, end_coin1_balance, start_coin1_value, end_coin1_value,
                        start_coin2_balance, end_coin2_balance, start_coin2_value, end_coin2_value,
                        start_coin3_balance, end_coin3_balance, start_coin3_value, end_coin3_value,
                        start_bnb_balance, end_bnb_balance, end_bnb_balance - start_bnb_balance,
                        start_bnb_value, end_bnb_value, end_bnb_value - start_bnb_value,
                        delta_coin1, delta_coin2, delta_coin3,
                        start_total_coin1_balance, end_total_coin1_balance,
                        start_total_coin2_balance, end_total_coin2_balance,
                        start_total_coin3_balance, end_total_coin3_balance,
                        pair1_executed, pair2_executed, pair3_executed,
                        pair1_top_off, pair2_top_off, pair3_top_off]
            log_string = ','.join(str(x) for x in log_list)
            print('log line: ', log_string)
            self.order_logger.info(log_string)

        # print(COIN1 + ' gain: ', end_coin1_value - start_coin1_value)
        # print(COIN2 + ' gain: ', end_coin2_value - start_coin2_value)
        # print(COIN3 + ' gain: ', end_coin3_value - start_coin3_value)
        print('total start: ', start_total_value, 'total end: ', end_total_value)
        print('total ' + self.COIN1 + ': ', end_total_coin1_balance, 'total ' + self.COIN2 + ': ', end_total_coin2_balance, 'total ' + self.COIN3 + ': ', end_total_coin3_balance)
        print('return: ', final_return, self.COIN1, (end_coin1_balance-start_coin1_balance), self.total_return)

        # Make sure we don't drop in value too much
        # TODO: This isn't working properly.  Account isn't being updated?
        self.all_time_high = max(self.all_time_high, end_total_value)
        if end_total_value < 0.60 * self.all_time_high and final_return < 0.0:
            self.exception_logger.error('Time: ' + datetime.utcnow().isoformat())
            self.exception_logger.error('Total value has dropped below 60% of all time high and lost on a trade')
            self.exception_logger.error('All Time High: ' + str(self.all_time_high) + 'Current Value: ' + str(end_total_value))
            print('Total value has dropped below 60% of all time high and lost on a trade')
            print('All Time High: ', self.all_time_high, 'Current Value: ', end_total_value)
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
                    self.exception_logger.error(self.COIN1 + ' Funds: ' + str(self.balance_book[self.COIN1]) + ' '
                                                + self.COIN2 + ' Funds: ' + str(self.balance_book[self.COIN2]) + ' '
                                                + self.COIN3 + ' Funds: ' + str(self.balance_book[self.COIN3]))
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


