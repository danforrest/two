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
    id = ''
    exchange = None
    direction = ''
    pair = None
    type = ''
    price = 0.0
    quantity = 0.0



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
        raise NotImplementedError('Exchange.market_convert_coins is abstract and must be implemented')


    def start_logging(self):
        self.log_start_time = datetime.utcnow().date()

        # self.order_logger = logging.getLogger('order_tracker')
        # self.order_logger.setLevel(logging.DEBUG)
        # self.exception_logger = logging.getLogger('exception_tracker')
        # self.exception_logger.setLevel(logging.DEBUG)
        # self.transaction_logger = logging.getLogger('transaction_tracker')
        # self.transaction_logger.setLevel(logging.DEBUG)
        #
        # base = 'logs\\'
        # order_log_file_name = '%sbinance_orders_%s.log' % (base, self.log_start_time.isoformat())
        # exception_log_file_name = '%sbinance_exceptions_%s.log' % (base, self.log_start_time.isoformat())
        # transaction_log_file_name = '%sbinance_transactions_%s.log' % (base, self.log_start_time.isoformat())
        # order_log_file_handler = logging.FileHandler(order_log_file_name)
        # order_log_file_handler.setLevel(logging.INFO)
        # exception_log_file_handler = logging.FileHandler(exception_log_file_name)
        # exception_log_file_handler.setLevel(logging.INFO)
        # transaction_log_file_handler = logging.FileHandler(transaction_log_file_name)
        # transaction_log_file_handler.setLevel(logging.INFO)
        #
        # # remove and existing log handlers and replace them with the ones we just created
        # for handler in self.order_logger.handlers[:]:
        #     self.order_logger.removeHandler(handler)
        # self.order_logger.addHandler(order_log_file_handler)
        # for handler in self.exception_logger.handlers[:]:
        #     self.exception_logger.removeHandler(handler)
        # self.exception_logger.addHandler(exception_log_file_handler)
        # for handler in self.transaction_logger.handlers[:]:
        #     self.transaction_logger.removeHandler(handler)
        # self.transaction_logger.addHandler(transaction_log_file_handler)


    def cancel_all_orders(self):
        raise NotImplementedError('Exchange.cancel_all_orders is abstract and must be implemented')


    @staticmethod
    def calculate_coin_delta(start_value, average_value, price):
        if abs(start_value - average_value) > Exchange.AVERAGE_TOLERANCE * average_value:
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
                    time.sleep(3)


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


    def check_logs(self):
        # restart all sockets if they've been up more than half a day
        current_time = datetime.utcnow().date()
        if current_time >= self.log_start_time + ONE_DAY:
            # starting the loggers will close down the old ones.
            self.start_logging()


