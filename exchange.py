import sys
import time
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

BNB = 'BNB'
BTC = 'BTC'
ETH = 'ETH'
NEO = 'NEO'
LTC = 'LTC'
USDT = 'USDT'

BTCUSDT = 'BTCUSDT'
ETHBTC = 'ETHBTC'
ETHUSDT = 'ETHUSDT'
NEOBTC = 'NEOBTC'
NEOETH = 'NEOETH'
NEOUSDT = 'NEOUSDT'
LTCUSDT = 'LTCUSDT'
LTCBTC = 'LTCBTC'
LTCETH = 'LTCETH'

COIN_LIST = [BTC, ETH, USDT, NEO, LTC]
PAIR_LIST = [BTCUSDT, ETHBTC, ETHUSDT, NEOBTC, NEOETH, NEOUSDT,
             LTCUSDT, LTCBTC, LTCETH]
CYCLE_LIST = [([BTC, ETH, USDT], [BTCUSDT, ETHUSDT, ETHBTC]),
              ([BTC, NEO, USDT], [BTCUSDT, NEOUSDT, NEOBTC]),
              ([ETH, NEO, USDT], [ETHUSDT, NEOUSDT, NEOETH]),
              ([BTC, ETH, NEO], [NEOBTC, NEOETH, ETHBTC]),
              ([BTC, LTC, USDT], [BTCUSDT, LTCUSDT, LTCBTC]),
              ([ETH, LTC, USDT], [ETHUSDT, LTCUSDT, LTCETH]),
              ([BTC, ETH, LTC], [LTCBTC, LTCETH, ETHBTC])]
PAIR_COINS = {BTCUSDT: (BTC, USDT),
              ETHBTC: (ETH, BTC),
              ETHUSDT: (ETH, USDT),
              NEOBTC: (NEO, BTC),
              NEOETH: (NEO, ETH),
              NEOUSDT: (NEO, USDT),
              LTCUSDT: (LTC, USDT),
              LTCBTC: (LTC, BTC),
              LTCETH: (LTC, ETH)}


class OrderBook:
    bid = 0.0
    ask = sys.maxsize
    bid_size = 0.0
    ask_size = 0.0


class CoinBalance:
    free = 0
    locked = 0


class Order:
    FILLED = 'FILLED'
    CANCELED = 'CANCELED'
    PENDING = 'PENDING'
    NONE = 'NONE'

    timestamp_placed = 0
    timestamp_cleared = 0
    timestamp = 0
    id = ''
    alt_id = ''
    exchange = None
    direction = ''
    pair = None
    reserve_coin = None
    type = ''
    price = 0.0
    quantity = 0.0
    executed_quantity = 0.0
    fee = {}
    status = NONE
    memo = 'NO_MEMO'
    sub_orders = []
    raw_order = None



class Exchange:

    api_key = 'None'
    api_secret = 'None'
    name = 'None'

    log_start_time = None

    exception_logger = None
    order_logger = None
    transaction_logger = None

    TICK = {ETHBTC: 0.0,
            NEOBTC: 0.0,
            NEOETH: 0.0,
            BTCUSDT: 0.0,
            ETHUSDT: 0.0,
            NEOUSDT: 0.0,
            LTCBTC: 0.0,
            LTCETH: 0.0,
            LTCUSDT: 0.0}
    PRICE_PRECISION = {ETHBTC: 8,
                       NEOBTC: 8,
                       NEOETH: 8,
                       BTCUSDT: 8,
                       ETHUSDT: 8,
                       NEOUSDT: 8,
                       LTCBTC: 8,
                       LTCETH: 8,
                       LTCUSDT: 8}
    PRICE_FORMAT = {ETHBTC: '%.8f',
                    NEOBTC: '%.8f',
                    NEOETH: '%.8f',
                    BTCUSDT: '%.8f',
                    ETHUSDT: '%.8f',
                    NEOUSDT: '%.8f',
                    LTCBTC: '%.8f',
                    LTCETH: '%.8f',
                    LTCUSDT: '%.8f'}
    QUANTITY_PRECISION = {ETHBTC: 8,
                          NEOBTC: 8,
                          NEOETH: 8,
                          BTCUSDT: 8,
                          ETHUSDT: 8,
                          NEOUSDT: 8,
                          LTCBTC: 8,
                          LTCETH: 8,
                          LTCUSDT: 8}
    MIN_AMOUNT = {ETHBTC: 1.0,
                  NEOBTC: 1.0,
                  NEOETH: 1.0,
                  BTCUSDT: 1.0,
                  ETHUSDT: 1.0,
                  NEOUSDT: 1.0,
                  LTCBTC: 1.0,
                  LTCETH: 1.0,
                  LTCUSDT: 1.0}

    FEE = 0.0025
    THRESHOLD = 1.008

    AVERAGE_TOLERANCE = 0.05

    raw_order_book_timestamp = None
    raw_order_book = None

    balance_book = None

    def __init__(self):
        self.raw_order_book_timestamp = None
        self.raw_order_book = {pair: OrderBook() for pair in PAIR_LIST}

        self.balance_book = {coin: CoinBalance() for coin in COIN_LIST}
        self.balance_book['timestamp'] = None
        self.balance_book['locked'] = False

        self.start_logging()

    total_return = 0.0
    all_time_high = 0.0


    def cancel_order(self, order):
        raise NotImplementedError('Exchange.cancel_order is abstract and must be implemented')

    @staticmethod
    def quick_calc(a_quantity, b_per_a, c_per_b, a_per_c):
        b_quantity = b_per_a * a_quantity
        c_quantity = c_per_b * b_quantity
        a_result = a_per_c * c_quantity

        return b_quantity, c_quantity, a_result


    def update_raw_order_book(self):
        raise NotImplementedError('Exchange.update_raw_order_book is abstract and must be implemented')


    def calculate_raw_coin_ratio(self, coin1, coin2):
        if coin1+coin2 in PAIR_LIST:
            coin1_per_coin2 = 1 / self.raw_order_book[coin1+coin2].ask
            coin2_per_coin1 = self.raw_order_book[coin1+coin2].bid
        elif coin2+coin1 in PAIR_LIST:
            coin2_per_coin1 = 1 / self.raw_order_book[coin2+coin1].ask
            coin1_per_coin2 = self.raw_order_book[coin2+coin1].bid
        else:
            error_string = 'No pairs found for coins', coin1, coin2, 'in: ', PAIR_LIST
            print(error_string)
            raise Exception(error_string)

        return coin1_per_coin2, coin2_per_coin1


    def market_convert_coins(self, coin1, coin2, quantity):
        raise NotImplementedError('Exchange.market_convert_coins is abstract and must be implemented')


    def update_order(self, order):
        raise NotImplementedError('Exchange.update_order is abstract and must be implemented')


    def query_coin_balances(self):
        raise NotImplementedError('Exchange.market_convert_coins is abstract and must be implemented')


    def start_logging(self):
        self.log_start_time = datetime.utcnow().date()

        # self.order_logger = logging.getLogger('order_tracker')
        # self.order_logger.setLevel(logging.DEBUG)
        self.exception_logger = logging.getLogger('exception_tracker')
        self.exception_logger.setLevel(logging.DEBUG)
        self.transaction_logger = logging.getLogger('transaction_tracker')
        self.transaction_logger.setLevel(logging.DEBUG)

        base = 'logs\\'
        # order_log_file_name = '%sbinance_orders_%s.log' % (base, self.log_start_time.isoformat())
        exception_log_file_name = '%smulti_exchange_exceptions_%s.log' % (base, self.log_start_time.isoformat())
        transaction_log_file_name = '%smulti_exchange_transactions_%s.log' % (base, self.log_start_time.isoformat())
        # order_log_file_handler = logging.FileHandler(order_log_file_name)
        # order_log_file_handler.setLevel(logging.INFO)
        exception_log_file_handler = logging.FileHandler(exception_log_file_name)
        exception_log_file_handler.setLevel(logging.INFO)
        transaction_log_file_handler = logging.FileHandler(transaction_log_file_name)
        transaction_log_file_handler.setLevel(logging.INFO)

        # remove and existing log handlers and replace them with the ones we just created
        # for handler in self.order_logger.handlers[:]:
        #     self.order_logger.removeHandler(handler)
        # self.order_logger.addHandler(order_log_file_handler)
        for handler in self.exception_logger.handlers[:]:
            self.exception_logger.removeHandler(handler)
        self.exception_logger.addHandler(exception_log_file_handler)
        for handler in self.transaction_logger.handlers[:]:
            self.transaction_logger.removeHandler(handler)
        self.transaction_logger.addHandler(transaction_log_file_handler)


    def cancel_all_orders(self):
        raise NotImplementedError('Exchange.cancel_all_orders is abstract and must be implemented')


    @staticmethod
    def calculate_coin_delta(start_value, average_value, price):
        if abs(start_value - average_value) > Exchange.AVERAGE_TOLERANCE * average_value:
            return (average_value - start_value) / (2 * price)
        else:
            return 0.0


    def check_logs(self):
        # restart all sockets if they've been up more than half a day
        current_time = datetime.utcnow().date()
        if current_time >= self.log_start_time + ONE_DAY:
            # starting the loggers will close down the old ones.
            self.start_logging()


