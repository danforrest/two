from cryptopia_api import Api
import sys
import time
import logging
from datetime import datetime, timedelta
import requests
import traceback
import gc
import copy
from exchange import Exchange, COIN_LIST, PAIR_LIST, CYCLE_LIST
from exchange import BTC, ETH, NEO, LTC, USDT
from exchange import BTCUSDT, ETHBTC, ETHUSDT, NEOBTC, NEOETH, NEOUSDT, LTCUSDT, LTCBTC, LTCETH

EXCHANGE = 'kucoin'

HALF_DAY = timedelta(hours=12)
ONE_DAY = timedelta(days=1)
FORWARD = 1
REVERSE = 0


class CryptopiaExchange(Exchange):
    api_key = 'a1d3619acd294cab8317c72326e4b626'
    api_secret = 'zpSMB5glgEWJDFVKVt7/d384Xj5Clv99qo8XTbGQMlY='

    log_start_time = None

    exception_logger = None
    order_logger = None
    transaction_logger = None

    client = None

    TICK = {ETHBTC: 0.00000001,
            NEOBTC: 0.00000001,
            NEOETH: 0.00000001,
            BTCUSDT: 0.00000001,
            ETHUSDT: 0.00000001,
            NEOUSDT: 0.00000001,
            LTCBTC: 0.00000001,
            LTCETH: 0.00000001,
            LTCUSDT: 0.00000001}
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
    MIN_AMOUNT = {ETHBTC: 0.00000001,
                  NEOBTC: 0.00000001,
                  NEOETH: 0.00000001,
                  BTCUSDT: 0.00000001,
                  ETHUSDT: 0.00000001,
                  NEOUSDT: 0.00000001,
                  LTCBTC: 0.00000001,
                  LTCETH: 0.00000001,
                  LTCUSDT: 0.00000001}
    MIN_NOTIONAL = {ETHBTC: 0.00000001,
                    NEOBTC: 0.00000001,
                    NEOETH: 0.00000001,
                    BTCUSDT: 0.00000001,
                    ETHUSDT: 0.00000001,
                    NEOUSDT: 0.00000001,
                    LTCBTC: 0.00000001,
                    LTCETH: 0.00000001,
                    LTCUSDT: 0.00000001}
    local_name = {}
    query_name = {}

    def __init__(self):
        super(CryptopiaExchange, self).__init__()
        self.local_name = {BTCUSDT: 'BTC/USDT',
                           ETHBTC: 'ETH/BTC',
                           ETHUSDT: 'ETH/USDT',
                           NEOBTC: 'NEO/BTC',
                           NEOETH: 'NEO/ETH',
                           NEOUSDT: 'NEO/USDT',
                           LTCUSDT: 'LTC/USDT',
                           LTCBTC: 'LTC/BTC',
                           LTCETH: 'LTC/ETH'}
        self.query_name = {BTCUSDT: 'BTC_USDT',
                           ETHBTC: 'ETH_BTC',
                           ETHUSDT: 'ETH_USDT',
                           NEOBTC: 'NEO_BTC',
                           NEOETH: 'NEO_ETH',
                           NEOUSDT: 'NEO_USDT',
                           LTCUSDT: 'LTC_USDT',
                           LTCBTC: 'LTC_BTC',
                           LTCETH: 'LTC_ETH'}

        self.client = Api(self.api_key, self.api_secret)
        self.FEE = 0.002
        self.THRESHOLD = 1.0062

        trade_pairs = self.client.get_tradepairs()
        for pair_info in trade_pairs[0]:
            pair_label = pair_info['Label']
            pair = pair_info['Symbol']+pair_info['BaseSymbol']
            if pair not in self.raw_order_book:
                continue
            if self.TICK[pair] != float(pair_info['MinimumTrade']):
                print('changing tick size for {} from {} to {}'.format(pair_label, self.TICK[pair], pair_info['MinimumTrade']))
            self.TICK[pair] = float(pair_info['MinimumTrade'])
            # PRICE_PRECISION
            # PRICE_FORMAT
            if self.MIN_AMOUNT[pair] != float(pair_info['MinimumTrade']):
                print('changing min amount for {} from {} to {}'.format(pair_label, self.MIN_AMOUNT[pair], pair_info['MinimumTrade']))
            self.MIN_AMOUNT[pair] = float(pair_info['MinimumTrade'])
            # QUANTITY_PRECISION


    def cancel_order(self, order):
        print('Canceled: ', order['symbol'])
        try:
            self.client.cancel_trade(trade_type='Trade', order_id=order['orderId'])
        except Exception as e:
            self.exception_logger.error('Time: ' + datetime.utcnow().isoformat())
            self.exception_logger.error(traceback.format_exc())
            time.sleep(3)
            raise e


    def update_raw_order_book(self):
        for pair in PAIR_LIST:
            try:
                order_book_tickers = self.client.get_orders(self.query_name[pair])
                print('order book tickers: ', order_book_tickers, self.query_name[pair])
                if order_book_tickers[0] is None:
                    print('Cryptopia: No update for {}'.format(pair))
                    continue
                order_book_tickers = order_book_tickers[0]
                if pair in self.raw_order_book:
                    if order_book_tickers['Buy'] is not None:
                        self.raw_order_book[pair].bid = order_book_tickers['Buy'][0]['Price']
                        self.raw_order_book[pair].bid_size = order_book_tickers['Buy'][0]['Volume']
                    else:
                        self.raw_order_book[pair].bid = 0.0
                        self.raw_order_book[pair].bid_size = 0.0
                    if order_book_tickers['Sell'] is not None:
                        self.raw_order_book[pair].ask = order_book_tickers['Sell'][0]['Price']
                        self.raw_order_book[pair].ask_size = order_book_tickers['Sell'][0]['Volume']
                    else:
                        self.raw_order_book[pair].ask = sys.float_info.max
                        self.raw_order_book[pair].ask_size = 0.0
            except Exception as e:
                traceback.print_exc()
                print('pair: ', pair, self.local_name[pair])
                print('exception: ', e)


    def update_raw_order_pair(self, pair):
        try:
            order_book_tickers = self.client.get_orders(self.query_name[pair], limit=1)
            if order_book_tickers[0] is None:
                print('Cryptopia: No update for {}'.format(pair))
                return
            order_book_tickers = order_book_tickers[0]
            if pair in self.raw_order_book:
                if order_book_tickers['Buy'] is not None:
                    self.raw_order_book[pair].bid = order_book_tickers['Buy'][0]['Price']
                    self.raw_order_book[pair].bid_size = order_book_tickers['Buy'][0]['Volume']
                else:
                    self.raw_order_book[pair].bid = 0.0
                    self.raw_order_book[pair].bid_size = 0.0
                if order_book_tickers['Sell'] is not None:
                    self.raw_order_book[pair].ask = order_book_tickers['Sell'][0]['Price']
                    self.raw_order_book[pair].ask_size = order_book_tickers['Sell'][0]['Volume']
                else:
                    self.raw_order_book[pair].ask = sys.float_info.max
                    self.raw_order_book[pair].ask_size = 0.0
        except Exception as e:
            traceback.print_exc()
            print('pair: ', pair, self.local_name[pair])
            print('exception: ', e)


    def market_convert_coins(self, coin1, coin2, quantity):
        return None
        # TODO: Need min amount in addition to min notional
        if coin1+coin2 in self.PAIR_LIST:
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
        elif coin2+coin1 in self.PAIR_LIST:
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


    def query_coin_balances(self):
        result = self.client.get_balance()
        self.balance_book['locked'] = False
        for asset in result:
            if asset['Symbol'] in COIN_LIST:
                self.balance_book[asset['Symbol']].free = asset['Available']
                if asset['HeldForTrades'] > 0:
                    self.balance_book[asset['Symbol']].locked = asset['HeldForTrades']
                    self.balance_book['locked'] = True
        self.balance_book['timestamp'] = datetime.utcnow()


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
        try:
            self.client.cancel_trade(trade_type='All')
        except Exception as e:
            raise e


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


    def check_arbitrage(self):
        all_transactions = []
        start_balance = {}
        self.query_coin_balances()
        for coin in self.balance_book:
            if coin in ['timestamp', 'locked']:
                continue
            start_balance[coin] = self.balance_book[coin].free
            print(coin + ' starting balance:', start_balance[coin])

        self.update_raw_order_book()

        for coins, pairs in CYCLE_LIST:
            print('coins: ', coins, 'pairs: ', pairs)
            COIN1, COIN2, COIN3 = coins
            PAIR1, PAIR2, PAIR3 = pairs
            # calculate balance of each coin
            price = {COIN1: 0.0, COIN2: 0.0, COIN3: 0.0}
            start_value = {COIN1: 0.0, COIN2: 0.0, COIN3: 0.0}
            delta = {COIN1: 0.0, COIN2: 0.0, COIN3: 0.0}
            base_quantity = {COIN1: 0.0, COIN2: 0.0, COIN3: 0.0}
            adjusted_quantity = {COIN1: 0.0, COIN2: 0.0, COIN3: 0.0}
            coin_per_coin = {COIN1: {COIN2: 0.0, COIN3: 0.0},
                             COIN2: {COIN1: 0.0, COIN3: 0.0},
                             COIN3: {COIN1: 0.0, COIN2: 0.0}}

            # calculate the value of each coin in dollars
            # for coin in price:
            #     if coin == 'USDT':
            #         price[coin] = 1.0
            #     else:
            #         price[coin] = self.raw_order_book[coin+'USDT'].ask
            #
            # for coin in start_value:
            #     start_value[coin] = start_balance[coin] * price[coin]
            #     # print(coin + ' value: ', start_value[coin])
            #
            # base_quantity[COIN1] = start_balance[COIN1]
            # base_value = base_quantity[COIN1] * price[COIN1]
            #
            # if start_value[COIN2] < base_value:
            #     # coin2 value is too low
            #     base_quantity[COIN1] = start_value[COIN2] / price[COIN1]
            #     base_value = start_value[COIN2]
            # if start_value[COIN3] < base_value:
            #     # coin3 value is too low
            #     base_quantity[COIN1] = start_value[COIN3] / price[COIN1]
            #
            # # don't use all our available coins
            # base_quantity[COIN1] *= 0.8
            # # print('start base quantity: ', base_quantity[COIN1])
            #
            # # adjust eth/btc quantities to re-balance funds if necessary.
            # average_value = (start_value[COIN1] + start_value[COIN2] + start_value[COIN3]) / 3.0
            # # print('average value: ', average_value)
            # # only adjust the coin values by half so we can try to get a little actual arbitrage
            # for coin in delta:
            #     delta[coin] = self.calculate_coin_delta(start_value[coin], average_value, price[coin])
            #     # print('delta ' + coin + ': ', delta[coin])

            order_start_time = datetime.utcnow()

            # pick a price in the middle of the spread and see if that works for arbitrage
            original_raw_order_book = copy.deepcopy(self.raw_order_book)

            # for pair in pairs:
            #     print('raw', pair + ' bid: ', self.PRICE_FORMAT[pair] % self.raw_order_book[pair].bid,
            #           ' ask: ', self.PRICE_FORMAT[pair] % self.raw_order_book[pair].ask)

            # coin1_per_coin2, coin2_per_coin1 = self.calculate_coin_ratio(COIN1, COIN2, original_order_book)
            # coin1_per_coin3, coin3_per_coin1 = self.calculate_coin_ratio(COIN1, COIN3, original_order_book)
            # coin2_per_coin3, coin3_per_coin2 = self.calculate_coin_ratio(COIN2, COIN3, original_order_book)

            coin_per_coin[COIN1][COIN2], coin_per_coin[COIN2][COIN1] = self.calculate_raw_coin_ratio(COIN1, COIN2)
            coin_per_coin[COIN1][COIN3], coin_per_coin[COIN3][COIN1] = self.calculate_raw_coin_ratio(COIN1, COIN3)
            coin_per_coin[COIN2][COIN3], coin_per_coin[COIN3][COIN2] = self.calculate_raw_coin_ratio(COIN2, COIN3)

            # print(COIN1 + '_per_' + COIN3 + ': ', coin1_per_coin3, coin3_per_coin1)
            # print(COIN3 + '_per_' + COIN2 + ': ', coin3_per_coin2, coin2_per_coin3)
            # print(COIN2 + '_per_' + COIN1 + ': ', coin2_per_coin1, coin1_per_coin2)

            # old_forward_arbitrage = coin1_per_coin3 * coin3_per_coin2 * coin2_per_coin1
            # old_reverse_arbitrage = coin1_per_coin2 * coin2_per_coin3 * coin3_per_coin1
            forward_arbitrage = coin_per_coin[COIN1][COIN3] * coin_per_coin[COIN3][COIN2] * coin_per_coin[COIN2][COIN1]
            reverse_arbitrage = coin_per_coin[COIN1][COIN2] * coin_per_coin[COIN2][COIN3] * coin_per_coin[COIN3][COIN1]

            # print('forward: ', '%.5f' % old_forward_arbitrage, '%.5f' % forward_arbitrage)
            # print('reverse: ', '%.5f' % old_reverse_arbitrage, '%.5f' % reverse_arbitrage)
            print('gain: ',
                  '%.3f' % ((max(forward_arbitrage, reverse_arbitrage)-1.0)*100),
                  '  (%.3f' % ((forward_arbitrage-1.0)*100),
                  '%.3f)' % ((reverse_arbitrage-1.0)*100))

            if max(forward_arbitrage, reverse_arbitrage) > self.THRESHOLD:
                print('******** Opportunity ********')
                # exit(0)

            if forward_arbitrage > reverse_arbitrage:
                direction = FORWARD
                gain = forward_arbitrage
                start_coin = COIN1
                mid_coin = COIN3
                end_coin = COIN2
                start_pair = PAIR1
                mid_pair = PAIR3
                end_pair = PAIR2
            else:
                direction = REVERSE
                gain = reverse_arbitrage
                start_coin = COIN1
                mid_coin = COIN2
                end_coin = COIN3
                start_pair = PAIR2
                mid_pair = PAIR3
                end_pair = PAIR1

        # pair_order = {}
        # pair1_order = self.EMPTY_ORDER
        # pair2_order = self.EMPTY_ORDER
        # pair3_order = self.EMPTY_ORDER
        # updated_order = {}
        # updated_pair1_order = self.EMPTY_ORDER
        # updated_pair2_order = self.EMPTY_ORDER
        # updated_pair3_order = self.EMPTY_ORDER
        # found_order = True
        #
        # order_timestamp = datetime.utcnow()
        # if direction == FORWARD and gain > self.THRESHOLD:
        #     print('doing forward arbitrage')
        #     base_quantity[end_coin], base_quantity[mid_coin], c1_result = self.quick_calc(base_quantity[start_coin],
        #                                                             coin_per_coin[end_coin][start_coin],
        #                                                             coin_per_coin[mid_coin][end_coin],
        #                                                             coin_per_coin[start_coin][mid_coin])
        #
        #     adjusted_quantity[start_coin] = base_quantity[start_coin] - delta[start_coin]
        #     adjusted_quantity[start_coin] += delta[end_coin] * coin_per_coin[start_coin][end_coin]
        #     adjusted_quantity[start_coin] = min(0.95*start_balance[start_coin], adjusted_quantity[start_coin])
        #     adjusted_quantity[end_coin] = base_quantity[end_coin] - delta[end_coin]
        #     adjusted_quantity[end_coin] += delta[mid_coin] * coin_per_coin[end_coin][mid_coin]
        #     adjusted_quantity[end_coin] = min(0.95*start_balance[end_coin], adjusted_quantity[end_coin])
        #     adjusted_quantity[mid_coin] = base_quantity[mid_coin] - delta[mid_coin]
        #     adjusted_quantity[mid_coin] += delta[start_coin] * coin_per_coin[mid_coin][start_coin]
        #     adjusted_quantity[mid_coin] = min(0.95*start_balance[mid_coin], adjusted_quantity[mid_coin])
        #
        #     if adjusted_quantity[self.COIN1] > 0:
        #         pair2_order = self.market_convert_coins(self.COIN1, self.COIN2, adjusted_quantity[self.COIN1])
        #     if adjusted_quantity[self.COIN2] > 0:
        #         pair3_order = self.market_convert_coins(self.COIN2, self.COIN3, adjusted_quantity[self.COIN2])
        #     if adjusted_quantity[self.COIN3] > 0:
        #         pair1_order = self.market_convert_coins(self.COIN3, self.COIN1, adjusted_quantity[self.COIN3])
        #
        #     # if adjusted_quantity[start_coin] > 0:
        #     #     pair_order[start_pair] = self.convert_coins(start_coin, mid_coin, adjusted_quantity[start_coin],
        #     #                                                   original_order_book)
        #     # if adjusted_quantity[mid_coin] > 0:
        #     #     pair_order[mid_pair] = self.convert_coins(mid_coin, end_coin, adjusted_quantity[mid_coin],
        #     #                                                 original_order_book)
        #     # if adjusted_quantity[end_coin] > 0:
        #     #     pair_order[end_pair] = self.convert_coins(end_coin, start_coin, adjusted_quantity[end_coin],
        #     #                                                 original_order_book)
        #
        #     print(self.COIN1 + ': ', base_quantity[self.COIN1], adjusted_quantity[self.COIN1])
        #     print(self.COIN2 + ': ', base_quantity[self.COIN2], adjusted_quantity[self.COIN2])
        #     print(self.COIN3 + ': ', base_quantity[self.COIN3], adjusted_quantity[self.COIN3])
        # elif direction == REVERSE and gain > self.THRESHOLD:
        #     print('doing reverse arbitrage')
        #     base_quantity[end_coin], base_quantity[mid_coin], c1_result = self.quick_calc(base_quantity[start_coin],
        #                                                             coin_per_coin[end_coin][start_coin],
        #                                                             coin_per_coin[mid_coin][end_coin],
        #                                                             coin_per_coin[start_coin][mid_coin])
        #
        #     adjusted_quantity[start_coin] = base_quantity[start_coin] - delta[start_coin]
        #     adjusted_quantity[start_coin] += delta[end_coin] * coin_per_coin[start_coin][end_coin]
        #     adjusted_quantity[start_coin] = min(0.95*start_balance[start_coin], adjusted_quantity[start_coin])
        #     adjusted_quantity[end_coin] = base_quantity[end_coin] - delta[end_coin]
        #     adjusted_quantity[end_coin] += delta[mid_coin] * coin_per_coin[end_coin][mid_coin]
        #     adjusted_quantity[end_coin] = min(0.95*start_balance[end_coin], adjusted_quantity[end_coin])
        #     adjusted_quantity[mid_coin] = base_quantity[mid_coin] - delta[mid_coin]
        #     adjusted_quantity[mid_coin] += delta[start_coin] * coin_per_coin[mid_coin][start_coin]
        #     adjusted_quantity[mid_coin] = min(0.95*start_balance[mid_coin], adjusted_quantity[mid_coin])
        #
        #     if adjusted_quantity[self.COIN1] > 0:
        #         pair1_order = self.market_convert_coins(self.COIN1, self.COIN3, adjusted_quantity[self.COIN1])
        #     if adjusted_quantity[self.COIN3] > 0:
        #         pair3_order = self.market_convert_coins(self.COIN3, self.COIN2, adjusted_quantity[self.COIN3])
        #     if adjusted_quantity[self.COIN2] > 0:
        #         pair2_order = self.market_convert_coins(self.COIN2, self.COIN1, adjusted_quantity[self.COIN2])
        #
        #     # if adjusted_quantity[start_coin] > 0:
        #     #     pair_order[start_pair] = self.convert_coins(start_coin, mid_coin, adjusted_quantity[start_coin],
        #     #                                                   original_order_book)
        #     # if adjusted_quantity[mid_coin] > 0:
        #     #     pair_order[mid_pair] = self.convert_coins(mid_coin, end_coin, adjusted_quantity[mid_coin],
        #     #                                                 original_order_book)
        #     # if adjusted_quantity[end_coin] > 0:
        #     #     pair_order[end_pair] = self.convert_coins(end_coin, start_coin, adjusted_quantity[end_coin],
        #     #                                                 original_order_book)
        #
        #     print(self.COIN1 + ': ', base_quantity[self.COIN1], adjusted_quantity[self.COIN1])
        #     print(self.COIN2 + ': ', base_quantity[self.COIN2], adjusted_quantity[self.COIN2])
        #     print(self.COIN3 + ': ', base_quantity[self.COIN3], adjusted_quantity[self.COIN3])
        # else:
        #     found_order = False
        #     print('no opportunity')
        #
        # if found_order:
        #     if direction == FORWARD:
        #         print(self.COIN3 + '->' + self.COIN1, self.COIN1 + '->' + self.COIN2, self.COIN2 + '->' + self.COIN3)
        #     else:
        #         print(self.COIN1 + '->' + self.COIN3, self.COIN2 + '->' + self.COIN1, self.COIN3 + '->' + self.COIN2)
        #
        #     # give the system 1 second for balances to be updated
        #     all_transactions.append(pair1_order)
        #     all_transactions.append(pair2_order)
        #     all_transactions.append(pair3_order)
        #     # for pair in pair_order:
        #     #     all_transactions.append(pair)
        #
        # order_end_time = datetime.utcnow()
        #
        # self.update_transaction_log(all_transactions)
        #
        # end_coin1_balance = 0.0
        # end_coin3_balance = 0.0
        # end_coin2_balance = 0.0
        # end_bnb_balance = 0.0
        # if found_order and (self.balance_book['timestamp'] < order_timestamp or self.balance_book['locked']):
        #     # the balance book hasn't been updated yet.  This is a minor problem
        #     # if we had actual orders (it should be logged).
        #     # if (pair_order[start_pair] is not None and pair_order[start_pair] != self.EMPTY_ORDER) or \
        #     #         (pair_order[mid_pair] is not None and pair_order[mid_pair] != self.EMPTY_ORDER) or \
        #     #         (pair_order[end_pair] is not None and pair_order[end_pair] != self.EMPTY_ORDER):
        #     if (pair1_order is not None and pair1_order != self.EMPTY_ORDER) or \
        #             (pair2_order is not None and pair2_order != self.EMPTY_ORDER) or \
        #             (pair3_order is not None and pair3_order != self.EMPTY_ORDER):
        #         self.exception_logger.warning('Time: ' + datetime.utcnow().isoformat())
        #         self.exception_logger.warning('Warning: Balance book was not updated')
        #         self.exception_logger.warning('Last updated at: ' + self.balance_book['timestamp'].isoformat())
        #         self.exception_logger.warning('Order timestamp: ' + order_timestamp.isoformat())
        #     assets_locked = True
        #     while assets_locked:
        #         assets_locked = False
        #         self.query_coin_balances()
        #         for coin in self.balance_book:
        #             if coin in [self.COIN1, self.COIN2, self.COIN3, 'BNB'] \
        #                     and self.balance_book[coin]['locked'] > 0:
        #                 print('Waiting for ' + coin + ' to be unlocked...')
        #                 # wait till all assets are freed.  sometimes binance can be slow.
        #                 self.exception_logger.warning('Time: ' + datetime.utcnow().isoformat())
        #                 self.exception_logger.warning('Warning: Assets still locked')
        #                 self.exception_logger.warning('Coin: ' + coin + ' Amount: ' + str(self.balance_book[coin]['locked']))
        #                 assets_locked = True
        #                 time.sleep(5.0)
        #                 continue
        #
        # for coin in self.balance_book:
        #     if coin == self.COIN1:
        #         end_coin1_balance = self.balance_book[coin]['free']
        #     elif coin == self.COIN2:
        #         end_coin2_balance = self.balance_book[coin]['free']
        #     elif coin == self.COIN3:
        #         end_coin3_balance = self.balance_book[coin]['free']
        #     elif coin == 'BNB':
        #         end_bnb_balance = self.balance_book[coin]['free']
        # end_coin1_value = end_coin1_balance * price[self.COIN1]
        # end_coin2_value = end_coin2_balance * price[self.COIN2]
        # end_coin3_value = end_coin3_balance * price[self.COIN3]
        # end_bnb_value = end_bnb_balance * price['BNB']
        #
        # pair1_filled = float(pair1_order['executedQty']) + float(updated_pair1_order['executedQty'])
        # pair2_filled = float(pair2_order['executedQty']) + float(updated_pair2_order['executedQty'])
        # pair3_filled = float(pair3_order['executedQty']) + float(updated_pair3_order['executedQty'])
        # if pair1_order != self.EMPTY_ORDER:
        #     pair1_filled /= float(pair1_order['origQty'])
        # if pair2_order != self.EMPTY_ORDER:
        #     pair2_filled /= float(pair2_order['origQty'])
        # if pair3_order != self.EMPTY_ORDER:
        #     pair3_filled /= float(pair3_order['origQty'])
        # # pair1_filled = float(pair_order[self.PAIR1]['executedQty']) + float(updated_order[self.PAIR1]['executedQty'])
        # # pair2_filled = float(pair_order[self.PAIR2]['executedQty']) + float(updated_order[self.PAIR2]['executedQty'])
        # # pair3_filled = float(pair_order[self.PAIR3]['executedQty']) + float(updated_order[self.PAIR3]['executedQty'])
        # # if pair_order[self.PAIR1] != self.EMPTY_ORDER:
        # #     pair1_filled /= float(pair_order[self.PAIR1]['origQty'])
        # # if pair_order[self.PAIR2] != self.EMPTY_ORDER:
        # #     pair2_filled /= float(pair_order[self.PAIR2]['origQty'])
        # # if pair_order[self.PAIR3] != self.EMPTY_ORDER:
        # #     pair3_filled /= float(pair_order[self.PAIR3]['origQty'])
        #
        # start_total_value = start_value[self.COIN1]+start_value[self.COIN2]+start_value[self.COIN3]+start_value['BNB']
        # end_total_value = end_coin1_value+end_coin2_value+end_coin3_value+end_bnb_value
        # if pair1_filled + pair2_filled + pair3_filled > 2.5:
        #     final_return = end_total_value-start_total_value
        #     self.total_return += final_return
        #     final_return = '%.4f' % final_return
        # else:
        #     final_return = '-'
        # start_total_coin1_balance = start_balance[self.COIN1] + (start_balance[self.COIN2] * coin_per_coin[self.COIN1][self.COIN2]) + (start_balance[self.COIN3] * coin_per_coin[self.COIN1][self.COIN3])
        # start_total_coin2_balance = (start_balance[self.COIN1] * coin_per_coin[self.COIN2][self.COIN1]) + start_balance[self.COIN2] + (start_balance[self.COIN3] * coin_per_coin[self.COIN2][self.COIN3])
        # start_total_coin3_balance = (start_balance[self.COIN1] * coin_per_coin[self.COIN3][self.COIN1]) + (start_balance[self.COIN2] * coin_per_coin[self.COIN3][self.COIN2]) + start_balance[self.COIN3]
        # end_total_coin1_balance = end_coin1_balance + (end_coin2_balance * coin_per_coin[self.COIN1][self.COIN2]) + (end_coin3_balance * coin_per_coin[self.COIN1][self.COIN3])
        # end_total_coin2_balance = (end_coin1_balance * coin_per_coin[self.COIN2][self.COIN1]) + end_coin2_balance + (end_coin3_balance * coin_per_coin[self.COIN2][self.COIN3])
        # end_total_coin3_balance = (end_coin1_balance * coin_per_coin[self.COIN3][self.COIN1]) + (end_coin2_balance * coin_per_coin[self.COIN3][self.COIN2]) + end_coin3_balance
        #
        # if found_order:
        #     print(self.COIN1 + ' ending diff:', end_coin1_balance - start_balance[self.COIN1])
        #     print(self.COIN2 + ' ending diff:', end_coin2_balance - start_balance[self.COIN2])
        #     print(self.COIN3 + ' ending diff:', end_coin3_balance - start_balance[self.COIN3])
        #     print('BNB ending diff:', end_bnb_balance - start_balance['BNB'])
        #
        #     log_list = ['binance', 'v1.4', order_start_time.isoformat(), order_end_time.isoformat(),
        #                 '%.4f' % (order_end_time.timestamp()-order_start_time.timestamp()),
        #                 self.PAIR1, self.PAIR2, self.PAIR3,
        #                 final_return, '%.4f' % start_total_value, '%.4f' % end_total_value,
        #                 original_raw_order_book[self.PAIR1].bid, original_raw_order_book[self.PAIR1].ask,
        #                 original_raw_order_book[self.PAIR2].bid, original_raw_order_book[self.PAIR2].ask,
        #                 original_raw_order_book[self.PAIR3].bid, original_raw_order_book[self.PAIR3].ask,
        #                 self.FEE, self.THRESHOLD, self.TOPOFF_THRESHOLD,
        #                 forward_arbitrage, reverse_arbitrage,
        #                 adjusted_quantity[self.COIN1], adjusted_quantity[self.COIN2], adjusted_quantity[self.COIN3],
        #                 pair1_order['status']=='FILLED', pair2_order['status']=='FILLED', pair3_order['status']=='FILLED',
        #                 start_balance[self.COIN1], end_coin1_balance, start_value[self.COIN1], end_coin1_value,
        #                 start_balance[self.COIN2], end_coin2_balance, start_value[self.COIN2], end_coin2_value,
        #                 start_balance[self.COIN3], end_coin3_balance, start_value[self.COIN3], end_coin3_value,
        #                 start_balance['BNB'], end_bnb_balance, end_bnb_balance - start_balance['BNB'],
        #                 start_value['BNB'], end_bnb_value, end_bnb_value - start_value['BNB'],
        #                 delta[self.COIN1], delta[self.COIN2], delta[self.COIN3],
        #                 start_total_coin1_balance, end_total_coin1_balance,
        #                 start_total_coin2_balance, end_total_coin2_balance,
        #                 start_total_coin3_balance, end_total_coin3_balance,
        #                 pair1_filled, pair2_filled, pair3_filled,]
        #     log_string = ','.join(str(x) for x in log_list)
        #     print('log line: ', log_string)
        #     self.order_logger.info(log_string)
        #
        # # print(COIN1 + ' gain: ', end_coin1_value - start_value[self.COIN1])
        # # print(COIN2 + ' gain: ', end_coin2_value - start_value[self.COIN2])
        # # print(COIN3 + ' gain: ', end_coin3_value - start_value[self.COIN3])
        # print('total start: ', start_total_value, 'total end: ', end_total_value)
        # print('total ' + self.COIN1 + ': ', end_total_coin1_balance, 'total ' + self.COIN2 + ': ', end_total_coin2_balance, 'total ' + self.COIN3 + ': ', end_total_coin3_balance)
        # print('return: ', final_return, self.COIN1, (end_coin1_balance-start_balance[self.COIN1]), self.total_return)
        #
        # # Make sure we don't drop in value too much
        # # TODO: This isn't working properly.  Account isn't being updated?
        # # self.all_time_high = max(self.all_time_high, end_total_value)
        # # if end_total_value < 0.60 * self.all_time_high and final_return < 0.0:
        # #     self.exception_logger.error('Time: ' + datetime.utcnow().isoformat())
        # #     self.exception_logger.error('Total value has dropped below 60% of all time high and lost on a trade')
        # #     self.exception_logger.error('All Time High: ' + str(self.all_time_high) + 'Current Value: ' + str(end_total_value))
        # #     print('Total value has dropped below 60% of all time high and lost on a trade')
        # #     print('All Time High: ', self.all_time_high, 'Current Value: ', end_total_value)
        #     # sys.exit(-1)
        #
        # if found_order:
        #     # pause a short bit so we can read the results
        #     # long term, this can be removed
        #     time.sleep(1)
        gc.collect()
        time.sleep(1.0)


    def check_logs(self):
        # restart all sockets if they've been up more than half a day
        current_time = datetime.utcnow().date()
        if current_time >= self.log_start_time + ONE_DAY:
            # starting the loggers will close down the old ones.
            self.start_logging()


    def run_arbitrage(self):
        global client

        print('start logging')
        self.start_logging()
        print('cancel orders')
        self.cancel_all_orders()
        print('query balances')
        self.query_coin_balances()
        print('all prepped')

        exception_count = 0
        while True:
            try:
                self.check_arbitrage()
                self.check_logs()
                exception_count = 0
            except Exception as e:
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
                    self.query_coin_balances()
                else:
                    time.sleep(3)
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
                self.client = Api(self.api_key, self.api_secret)
                self.cancel_all_orders()
                self.query_coin_balances()
            except Exception as e:
                print('Exitting on exception: ', e)
                self.exception_logger.error('Time: ' + datetime.utcnow().isoformat())
                self.exception_logger.error(traceback.format_exc())
                time.sleep(3)
                self.client = Api(self.api_key, self.api_secret)
                self.cancel_all_orders()
                self.query_coin_balances()


if __name__ == "__main__":
    exception_count = 0
    while True:
        try:
            start_time = datetime.utcnow()
            cryptopia_arbitrage = CryptopiaExchange()
            print('run arb')
            cryptopia_arbitrage.run_arbitrage()
            print('finish arb')
        except Exception as e:
            print('Failure at the top level', str(e))
            exception_time = datetime.utcnow()
            if exception_time - start_time < timedelta(minutes=30):
                if exception_count > 3:
                    time.sleep(3)
                    raise e
                else:
                    exception_count += 1
            else:
                exception_count = 0
            cryptopia_arbitrage = None
            time.sleep(60)
            gc.collect()
            time.sleep(60)


