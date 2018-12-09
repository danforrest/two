import krakenex
import sys
import time
import logging
from datetime import datetime, timedelta
import requests

HALF_DAY = timedelta(hours=12)
ONE_DAY = timedelta(days=1)


class OrderBook:
    bid = 0
    ask = sys.maxsize


class KrakenArbitrage():
    # pair priorities USDT always comes last in a pair.  BTC always comes after all
    # coins other than USDT.  ETH comes after all coins other than USDT and BTC.
    # also, pair should go COIN1/COIN3, COIN2/COIN1, COIN2/COIN3
    COIN1 = 'XXBT'
    COIN2 = 'XETH'
    COIN3 = 'ZUSD'
    PAIR1 = 'XXBTZUSD'
    PAIR2 = 'XETHXXBT'
    PAIR3 = 'XETHZUSD'

    #socket_start_time = None
    log_start_time = None

    exception_logger = None
    order_logger = None

    client = krakenex.API()
    client.load_key('kraken.key')

    EMPTY_ORDER = {'status': 'None',
                   'id': 'None',
                   'price': 0.0,
                   'size': 0.0,
                   'filled_size': 0.0,
                   'side': 'None'}

    TICK = {'XETHXXBT': 0.00001,
            'XXBTZUSD': 0.01,
            'XETHZUSD': 0.01}
    PRICE_PRECISION = {'XETHXXBT': 5,
                       'XXBTZUSD': 2,
                       'XETHZUSD': 2}
    PRICE_FORMAT = {'XETHXXBT': '%.5f',
                    'XXBTZUSD': '%.2f',
                    'XETHZUSD': '%.2f'}
    QUANTITY_PRECISION = {'XETHXXBT': 8,
                          'XXBTZUSD': 8,
                          'XETHZUSD': 8}
    SPREAD_THRESHOLD = {'XETHXXBT': 0.75,
                        'XXBTZUSD': 0.75,
                        'XETHZUSD': 0.75}
    MIN_AMOUNT = {'XETHXXBT': 0.01,
                  'XXBTZUSD': 0.001,
                  'XETHZUSD': 0.01}
    # MIN_NOTIONAL = {'XETHXXBT': 0.001,
    #                 'XXBTZUSD': 1.0,
    #                 'XETHZUSD': 20.0}

    FEE = 0.0
    THRESHOLD = 1.0010 # + (4 * FEE)

    raw_order_book = {'XETHXXBT': OrderBook(),
                      'XXBTZUSD': OrderBook(),
                      'XETHZUSD': OrderBook()}

    balance_book = {'timestamp': None,
                    'XETH': 0.0,
                    'XXBT': 0.0,
                    'ZUSD': 0.0}

    trade_order_book = {}

    total_return = 0.0
    all_time_high = 0.0


    # def update_order(self, order, check_level):
    #     if order['status'] == 'FILLED':
    #         return None
    #
    #     new_order = None
    #     price = 0.0
    #
    #     order_quantity = float(order['origQty'])
    #     executed_quantity = float(order['executedQty'])
    #     if (order_quantity - executed_quantity) < 10*self.MIN_AMOUNT[order['symbol']]:
    #         # May hit MIN NOTIONAL error if we try to re-submit the order for the
    #         # remaining amount.  Instead, let it ride, we shouldn't lose too much.
    #         return order
    #     new_quantity = round(order_quantity - executed_quantity,
    #                          self.QUANTITY_PRECISION[order['symbol']])
    #
    #     # don't re-place the order if the price isn't going to change.  we will just
    #     # lose our place in line in the order book and damage our fills per order+cancels
    #     # metrics.
    #     if order['side'] == 'BUY':
    #         if self.raw_order_book[order['symbol']].bid == float(order['price']):
    #             return order
    #         price = self.raw_order_book[order['symbol']].bid
    #         if check_level >= 2:
    #             price = round(price + self.TICK[order['symbol']], self.PRICE_PRECISION[order['symbol']])
    #     elif order['side'] == 'SELL':
    #         if self.raw_order_book[order['symbol']].ask == float(order['price']):
    #             return order
    #         price = self.raw_order_book[order['symbol']].ask
    #         if check_level >= 2:
    #             price = round(price - self.TICK[order['symbol']], self.PRICE_PRECISION[order['symbol']])
    #
    #     if price * new_quantity < self.MIN_NOTIONAL[order['symbol']]:
    #         print('Value under min notional')
    #         print('Price: ', price, 'quantity: ', new_quantity, 'min: ', self.MIN_NOTIONAL[order['symbol']])
    #         return order
    #
    #     try:
    #         self.client.cancel_order(symbol=order['symbol'], orderId=order['orderId'])
    #     except exceptions.BinanceAPIException as e:
    #         if e.message == 'UNKNOWN_ORDER' or e.code == -2011:
    #             print('Order already filled')
    #             return
    #         else:
    #             self.exception_logger.info('Time: ' + datetime.utcnow().isoformat())
    #             self.exception_logger.info(e)
    #             raise e
    #     if order['side'] == 'BUY':
    #         print('updating bid to: ', price, new_quantity)
    #         new_order = self.client.order_limit_buy(symbol=order['symbol'],
    #                                                 price=str(price),
    #                                                 quantity=new_quantity)
    #     elif order['side'] == 'SELL':
    #         print('updating ask to: ', price, new_quantity)
    #         new_order = self.client.order_limit_sell(symbol=order['symbol'],
    #                                                  price=str(price),
    #                                                  quantity=new_quantity)
    #     return new_order


    def cancel_order(self, order):
        # new_quantity = round(float(order['origQty']) - float(order['executedQty']), 2)
        print('Cancel: ', order)
        try:
            self.client.cancel_order(order['id'])
            # self.client.cancel_order(symbol=order['symbol'], orderId=order['orderId'])
        except Exception as e:
            # if e.message == 'UNKNOWN_ORDER' or e.code == -2011:
            #     print('Order already filled')
            #     return
            # else:
                self.exception_logger.error('Time: ' + datetime.utcnow().isoformat())
                self.exception_logger.error(e)
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
        self.trade_order_book = {self.PAIR1: OrderBook(),
                                 self.PAIR2: OrderBook(),
                                 self.PAIR3: OrderBook()}
        self.trade_order_book[self.PAIR1].bid = max(self.raw_order_book[self.PAIR1].bid,
                                                    self.raw_order_book[self.PAIR1].ask - (
                                                      self.SPREAD_THRESHOLD[self.PAIR1] * pair1_spread * self.TICK[self.PAIR1]))
        self.trade_order_book[self.PAIR1].ask = min(self.raw_order_book[self.PAIR1].ask,
                                                    self.raw_order_book[self.PAIR1].bid + (
                                                        self.SPREAD_THRESHOLD[self.PAIR1] * pair1_spread * self.TICK[self.PAIR1]))
        self.trade_order_book[self.PAIR2].bid = max(self.raw_order_book[self.PAIR2].bid,
                                                    self.raw_order_book[self.PAIR2].ask - (
                                                        self.SPREAD_THRESHOLD[self.PAIR2] * pair2_spread * self.TICK[self.PAIR2]))
        self.trade_order_book[self.PAIR2].ask = min(self.raw_order_book[self.PAIR2].ask,
                                                    self.raw_order_book[self.PAIR2].bid + (
                                                       self.SPREAD_THRESHOLD[self.PAIR2] * pair2_spread * self.TICK[self.PAIR2]))
        self.trade_order_book[self.PAIR3].bid = max(self.raw_order_book[self.PAIR3].bid,
                                                    self.raw_order_book[self.PAIR3].ask - (
                                                        self.SPREAD_THRESHOLD[self.PAIR3] * pair3_spread * self.TICK[self.PAIR3]))
        self.trade_order_book[self.PAIR3].ask = min(self.raw_order_book[self.PAIR3].ask,
                                                    self.raw_order_book[self.PAIR3].bid + (
                                                        self.SPREAD_THRESHOLD[self.PAIR3] * pair3_spread * self.TICK[self.PAIR3]))

        self.trade_order_book[self.PAIR1].bid = round(self.trade_order_book[self.PAIR1].bid,
                                                      self.PRICE_PRECISION[self.PAIR1])
        self.trade_order_book[self.PAIR1].ask = round(self.trade_order_book[self.PAIR1].ask,
                                                      self.PRICE_PRECISION[self.PAIR1])
        self.trade_order_book[self.PAIR2].bid = round(self.trade_order_book[self.PAIR2].bid,
                                                      self.PRICE_PRECISION[self.PAIR2])
        self.trade_order_book[self.PAIR2].ask = round(self.trade_order_book[self.PAIR2].ask,
                                                      self.PRICE_PRECISION[self.PAIR2])
        self.trade_order_book[self.PAIR3].bid = round(self.trade_order_book[self.PAIR3].bid,
                                                      self.PRICE_PRECISION[self.PAIR3])
        self.trade_order_book[self.PAIR3].ask = round(self.trade_order_book[self.PAIR3].ask,
                                                      self.PRICE_PRECISION[self.PAIR3])

        print(self.PAIR1 + ' bid: ', self.PRICE_FORMAT[self.PAIR1] % self.trade_order_book[self.PAIR1].bid,
              self.PAIR1 + ' ask: ', self.PRICE_FORMAT[self.PAIR1] % self.trade_order_book[self.PAIR1].ask)
        print(self.PAIR2 + ' bid: ', self.PRICE_FORMAT[self.PAIR2] % self.trade_order_book[self.PAIR2].bid,
              self.PAIR2 + ' ask: ', self.PRICE_FORMAT[self.PAIR2] % self.trade_order_book[self.PAIR2].ask)
        print(self.PAIR3 + ' bid: ', self.PRICE_FORMAT[self.PAIR3] % self.trade_order_book[self.PAIR3].bid,
              self.PAIR3 + ' ask: ', self.PRICE_FORMAT[self.PAIR3] % self.trade_order_book[self.PAIR3].ask)


    def calculate_coin_ratio(self, coin1, coin2):
        # TODO: Can't just use c1+c2
        if coin1+coin2 in [self.PAIR1, self.PAIR2, self.PAIR3]:
            pair = coin1+coin2
            coin1_per_coin2 = 1 / self.trade_order_book[pair].bid
            coin2_per_coin1 = self.trade_order_book[pair].ask
        elif coin2+coin1 in [self.PAIR1, self.PAIR2, self.PAIR3]:
            pair = coin2+coin1
            coin2_per_coin1 = 1 / self.trade_order_book[pair].bid
            coin1_per_coin2 = self.trade_order_book[pair].ask
        else:
            error_string = 'No pairs found for coins', coin1, coin2, 'in: ', self.PAIR1, self.PAIR2, self.PAIR3
            print(error_string)
            raise Exception(error_string)

        return coin1_per_coin2, coin2_per_coin1


    def convert_coins(self, coin1, coin2, quantity):
        # TODO: Need min amount in addition to min notional
        print('convert ', coin1, 'to', coin2, 'quantity: ', quantity)
        return None
        if coin1+coin2 in [self.PAIR1, self.PAIR2, self.PAIR3]:
            # sell
            pair = coin1+coin2
            price = self.PRICE_FORMAT[pair] % self.trade_order_book[pair].ask
            adjusted_quantity = round(quantity, self.QUANTITY_PRECISION[pair])
            # print('SELL: ', pair, 'price: ', price, 'quantity: ', adjusted_quantity)
            if adjusted_quantity < self.MIN_AMOUNT[pair]:
                print('Value under min quantity')
                print('Price: ', price, 'quantity: ', adjusted_quantity, 'min: ', self.MIN_AMOUNT[pair])
                return None
            try:
                order = self.client.sell(product_id=pair,
                                              price=price,
                                              size=str(round(adjusted_quantity,
                                                             self.QUANTITY_PRECISION[pair])),
                                              post_only=True)
            except Exception as e:
                self.exception_logger.error('Time: ' + datetime.utcnow().isoformat())
                self.exception_logger.error('Exception placing an order')
                self.exception_logger.error('Coin1: ' + coin1 + ' Coin2: ' + coin2 + ' Quantity: ' + str(quantity))
                self.exception_logger.error('Sell Pair: ' + pair + ' Price: ' + str(price) + ' Adjusted Quantity: ' + str(adjusted_quantity))
                self.exception_logger.error(e)
                print('Exception placing order', e)
                raise e
                #order = None
        elif coin2+coin1 in [self.PAIR1, self.PAIR2, self.PAIR3]:
            # buy
            pair = coin2+coin1
            price = self.PRICE_FORMAT[pair] % self.trade_order_book[pair].bid
            coin1_per_coin2, coin2_per_coin1 = self.calculate_coin_ratio(coin1, coin2)
            adjusted_quantity = round(quantity * coin2_per_coin1, self.QUANTITY_PRECISION[pair])
            # convert coin1 quantity to coin2 quantity
            # print('BUY: ', pair, 'price: ', price, 'quantity: ', adjusted_quantity)
            if adjusted_quantity < self.MIN_AMOUNT[pair]:
                print('Value under min amount')
                print('Price: ', price, 'quantity: ', adjusted_quantity, 'min: ', self.MIN_AMOUNT[pair])
                return None
            try:
                order = self.client.buy(product_id=pair,
                                             price=price,
                                             size=str(round(adjusted_quantity,
                                                            self.QUANTITY_PRECISION[pair])),
                                             post_only=True)
            except Exception as e:
                self.exception_logger.error('Time: ' + datetime.utcnow().isoformat())
                self.exception_logger.error('Exception placing an order')
                self.exception_logger.error('Coin1: ' + coin1 + ' Coin2: ' + coin2 + ' Quantity: ' + str(quantity))
                self.exception_logger.error('Buy Pair: ' + pair + ' Price: ' + str(price) + ' Adjusted Quantity: ' + str(adjusted_quantity))
                self.exception_logger.error(e)
                print('Exception placing order', e)
                raise e
                #order = None
        else:
            order = None

        print('Order: ', order)
        return order


    @staticmethod
    def print_order_status(pair1_order, pair2_order, pair3_order):
        status_string = 'Status:  '
        if pair1_order is None or pair1_order['status'] == 'None':
            status_string += '---  '
        else:
            status_string += '{:3d}  '.format(round(100*float(pair1_order['filled_size'])/float(pair1_order['size'])))
        if pair2_order is None or pair2_order['status'] == 'None':
            status_string += '---  '
        else:
            status_string += '{:3d}  '.format(round(100*float(pair2_order['filled_size'])/float(pair2_order['size'])))
        if pair3_order is None or pair3_order['status'] == 'None':
            status_string += '---  '
        else:
            status_string += '{:3d}  '.format(round(100*float(pair3_order['filled_size'])/float(pair3_order['size'])))

        print(status_string)


    def query_coin_balances(self):
        accounts = self.client.query_private('Balance')
        print('query coin balance: ', accounts)
        self.balance_book[self.COIN1] = 0
        self.balance_book[self.COIN2] = 0
        self.balance_book[self.COIN3] = 0
        for asset in accounts['result']:
            if asset['currency'] == self.COIN1:
                self.balance_book[asset['currency']] += float(asset['available'])
            elif asset['currency'] == self.COIN2:
                self.balance_book[asset['currency']] += float(asset['available'])
            elif asset['currency'] == self.COIN3:
                self.balance_book[asset['currency']] += float(asset['available'])
        self.balance_book['timestamp'] = datetime.utcnow()


    def query_order_book(self):
        pair_list = self.PAIR1 + ',' + self.PAIR2 + ',' + self.PAIR3
        pair_tickers = self.client.query_public('Ticker', {'pair': pair_list})
        for pair in pair_tickers['result']:
            self.raw_order_book[pair].bid = float(pair_tickers['result'][pair]['b'][0])
            self.raw_order_book[pair].ask = float(pair_tickers['result'][pair]['a'][0])


    def start_logging(self):
        self.log_start_time = datetime.utcnow().date()

        self.order_logger = logging.getLogger('kraken_order_tracker')
        self.order_logger.setLevel(logging.DEBUG)
        self.exception_logger = logging.getLogger('kraken_exception_tracker')
        self.exception_logger.setLevel(logging.DEBUG)

        order_log_file_handler = logging.FileHandler('logs\kraken_order_tracker_%s.log' % self.log_start_time.isoformat())
        order_log_file_handler.setLevel(logging.INFO)
        exception_log_file_handler = logging.FileHandler('logs\kraken_exception_tracker_%s.log' % self.log_start_time.isoformat())
        exception_log_file_handler.setLevel(logging.INFO)

        # remove and existing log handlers and replace them with the ones we just created
        for handler in self.order_logger.handlers[:]:
            self.order_logger.removeHandler(handler)
        self.order_logger.addHandler(order_log_file_handler)
        for handler in self.exception_logger.handlers[:]:
            self.exception_logger.removeHandler(handler)
        self.exception_logger.addHandler(exception_log_file_handler)


    def cancel_all_orders(self):
        self.client.cancel_all(product=self.PAIR1)
        self.client.cancel_all(product=self.PAIR2)
        self.client.cancel_all(product=self.PAIR3)


    @staticmethod
    def calculate_coin_delta(start_value, average_value, price):
        if abs(start_value - average_value) > 0.05 * average_value:
            return (average_value - start_value) / (2 * price)
        else:
            return 0.0


    def check_arbitrage(self):
        # calculate balance of each coin
        start_coin1_balance = 0.0
        start_coin2_balance = 0.0
        start_coin3_balance = 0.0
        for coin in self.balance_book:
            if coin == self.COIN1:
                start_coin1_balance = self.balance_book[coin]
            elif coin == self.COIN2:
                start_coin2_balance = self.balance_book[coin]
            elif coin == self.COIN3:
                start_coin3_balance = self.balance_book[coin]

        # calculate the value of each coin in dollars
        self.query_order_book()
        if self.COIN1+'ZUSD' in self.raw_order_book:
            coin1_price = self.raw_order_book[self.COIN1+'ZUSD'].ask
        if self.COIN2+'ZUSD' in self.raw_order_book:
            coin2_price = self.raw_order_book[self.COIN2+'ZUSD'].ask
        if self.COIN3+'ZUSD' in self.raw_order_book:
            coin3_price = self.raw_order_book[self.COIN3+'ZUSD'].ask
        # USD prices are always 1.0 coin per dollar
        print('coin3: ', self.COIN3)
        if self.COIN1 == 'ZUSD':
            coin1_price = 1.0
        elif self.COIN2 == 'ZUSD':
            coin2_price = 1.0
        elif self.COIN3 == 'ZUSD':
            coin3_price = 1.0

        start_coin1_value = start_coin1_balance * coin1_price
        start_coin2_value = start_coin2_balance * coin2_price
        start_coin3_value = start_coin3_balance * coin3_price

        # max_value = 100.0
        # if start_coin1_value > max_value:
        #     start_coin1_balance = max_value / coin1_price
        #     start_coin1_value = max_value
        # if start_coin2_value > max_value:
        #     start_coin2_balance = max_value / coin2_price
        #     start_coin2_value = max_value
        # if start_coin3_value > max_value:
        #     start_coin3_balance = max_value / coin3_price
        #     start_coin3_value = max_value

        print(self.COIN1 + ' starting balance:', start_coin1_balance)
        print(self.COIN2 + ' starting balance:', start_coin2_balance)
        print(self.COIN3 + ' starting balance:', start_coin3_balance)

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
        self.build_trade_order_book()

        coin1_per_coin2, coin2_per_coin1 = self.calculate_coin_ratio(self.COIN1, self.COIN2)
        coin1_per_coin3, coin3_per_coin1 = self.calculate_coin_ratio(self.COIN1, self.COIN3)
        coin2_per_coin3, coin3_per_coin2 = self.calculate_coin_ratio(self.COIN2, self.COIN3)

        print(self.COIN1 + '_per_' + self.COIN3 + ': ', coin1_per_coin3, coin3_per_coin1)
        print(self.COIN3 + '_per_' + self.COIN2 + ': ', coin3_per_coin2, coin2_per_coin3)
        print(self.COIN2 + '_per_' + self.COIN1 + ': ', coin2_per_coin1, coin1_per_coin2)

        forward_arbitrage = coin1_per_coin3 * coin3_per_coin2 * coin2_per_coin1
        reverse_arbitrage = coin1_per_coin2 * coin2_per_coin3 * coin3_per_coin1

        print('forward: ', forward_arbitrage)
        print('reverse: ', reverse_arbitrage)

        coin2_quantity = 0.0
        coin3_quantity = 0.0
        coin1_result = 0.0
        pair3_order = None
        pair2_order = None
        pair1_order = None
        found_order = True

        order_timestamp = datetime.utcnow()
        if forward_arbitrage > reverse_arbitrage and forward_arbitrage > self.THRESHOLD:
            print('doing forward arbitrage')
            coin2_quantity, coin3_quantity, coin1_result = self.quick_calc(base_quantity,
                                                                           coin2_per_coin1,
                                                                           coin3_per_coin2,
                                                                           coin1_per_coin3)
            pair1_price = self.PRICE_FORMAT[self.PAIR1] % self.trade_order_book[self.PAIR1].bid
            pair2_price = self.PRICE_FORMAT[self.PAIR2] % self.trade_order_book[self.PAIR2].ask
            pair3_price = self.PRICE_FORMAT[self.PAIR3] % self.trade_order_book[self.PAIR3].ask
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
                pair2_order = self.convert_coins(self.COIN1, self.COIN2, adjusted_coin1_quantity)
            if adjusted_coin2_quantity > 0:
                pair3_order = self.convert_coins(self.COIN2, self.COIN3, adjusted_coin2_quantity)
            if adjusted_coin3_quantity > 0:
                pair1_order = self.convert_coins(self.COIN3, self.COIN1, adjusted_coin3_quantity)
        elif reverse_arbitrage > forward_arbitrage and reverse_arbitrage > self.THRESHOLD:
            print('doing reverse arbitrage')
            coin3_quantity, coin2_quantity, coin1_result = self.quick_calc(base_quantity,
                                                                           coin3_per_coin1,
                                                                           coin2_per_coin3,
                                                                           coin1_per_coin2)
            pair1_price = self.PRICE_FORMAT[self.PAIR1] % self.trade_order_book[self.PAIR1].ask
            pair2_price = self.PRICE_FORMAT[self.PAIR2] % self.trade_order_book[self.PAIR2].bid
            pair3_price = self.PRICE_FORMAT[self.PAIR3] % self.trade_order_book[self.PAIR3].bid
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
                pair1_order = self.convert_coins(self.COIN1, self.COIN3, adjusted_coin1_quantity)
            if adjusted_coin3_quantity > 0:
                pair3_order = self.convert_coins(self.COIN3, self.COIN2, adjusted_coin3_quantity)
            if adjusted_coin2_quantity > 0:
                pair2_order = self.convert_coins(self.COIN2, self.COIN1, adjusted_coin2_quantity)
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
            while (pair1_order['status'] not in ['done', 'None']
                   or pair2_order['status'] not in ['done', 'None']
                   or pair3_order['status'] not in ['done', 'None'])\
                  and start_time + 120 > time.time():
                self.print_order_status(pair1_order, pair2_order, pair3_order)
                #print('Status: ', pair1_order['status'], pair2_order['status'], pair3_order['status'])
                check_count += 1
                time.sleep(3)
                if pair1_order != self.EMPTY_ORDER and pair1_order['status'] != 'done':
                    pair1_order = self.client.get_order(pair1_order['id'])
                    # if check_count % 4 == 0 and pair1_order['status'] != 'FILLED':
                    #     # reset the price
                    #     pair1_order = update_order(pair1_order, base_quantity, check_count / 4)
                if pair2_order != self.EMPTY_ORDER and pair2_order['status'] != 'done':
                    pair2_order = self.client.get_order(pair2_order['id'])
                    # if check_count % 4 == 0 and pair2_order['status'] != 'FILLED':
                    #     # reset the price
                    #     pair2_order = update_order(pair2_order, coin2_quantity, check_count/4)
                if pair3_order != self.EMPTY_ORDER and pair3_order['status'] != 'done':
                    pair3_order = self.client.get_order(pair3_order['id'])
                    # if check_count % 4 == 0 and pair3_order['status'] != 'FILLED':
                    #     # reset the price
                    #     pair3_order = update_order(pair3_order, base_quantity, check_count / 4)
            if pair1_order != self.EMPTY_ORDER and pair1_order['status'] != 'done':
                print('cancel pair1_order')
                self.cancel_order(pair1_order)
                pair1_order['status'] = 'canceled'
            if pair2_order != self.EMPTY_ORDER and pair2_order['status'] != 'done':
                print('cancel pair2_order')
                self.cancel_order(pair2_order)
                pair2_order['status'] = 'canceled'
            if pair3_order != self.EMPTY_ORDER and pair3_order['status'] != 'done':
                print('cancel pair3_order')
                self.cancel_order(pair3_order)
                pair3_order['status'] = 'canceled'
            # give the system 1 second for balances to be updated
            time.sleep(1)

        order_end_time = datetime.utcnow().isoformat()

        end_coin1_balance = 0.0
        end_coin3_balance = 0.0
        end_coin2_balance = 0.0
        if found_order and self.balance_book['timestamp'] < order_timestamp:
            # # the balance book hasn't been updated yet.  This is a minor problem
            # # if we had actual orders (it should be logged).
            # if (pair1_order is not None and pair1_order != self.EMPTY_ORDER) or \
            #         (pair2_order is not None and pair2_order != self.EMPTY_ORDER) or \
            #         (pair3_order is not None and pair3_order != self.EMPTY_ORDER):
            #     self.exception_logger.warning('Time: ' + datetime.utcnow().isoformat())
            #     self.exception_logger.warning('Warning: Balance book was not updated')
            #     self.exception_logger.warning('Last updated at: ' + self.balance_book['timestamp'].isoformat())
            #     self.exception_logger.warning('Order timestamp: ' + order_timestamp.isoformat())
            self.query_coin_balances()

        for coin in self.balance_book:
            if coin == self.COIN1:
                end_coin1_balance = self.balance_book[coin]
            elif coin == self.COIN2:
                end_coin2_balance = self.balance_book[coin]
            elif coin == self.COIN3:
                end_coin3_balance = self.balance_book[coin]
        end_coin1_value = end_coin1_balance * coin1_price
        end_coin2_value = end_coin2_balance * coin2_price
        end_coin3_value = end_coin3_balance * coin3_price

        start_total_value = start_coin1_value+start_coin2_value+start_coin3_value
        end_total_value = end_coin1_value+end_coin2_value+end_coin3_value
        final_return = end_total_value-start_total_value
        self.total_return += final_return
        total_coin1_balance = end_coin1_balance + (end_coin2_balance * coin1_per_coin2) + (end_coin3_balance * coin1_per_coin3)
        total_coin2_balance = (end_coin1_balance * coin2_per_coin1) + end_coin2_balance + (end_coin3_balance * coin2_per_coin3)
        total_coin3_balance = (end_coin1_balance * coin3_per_coin1) + (end_coin2_balance * coin3_per_coin2) + end_coin3_balance

        if found_order:
            print(self.COIN1 + ' ending diff:', end_coin1_balance - start_coin1_balance)
            print(self.COIN2 + ' ending diff:', end_coin2_balance - start_coin2_balance)
            print(self.COIN3 + ' ending diff:', end_coin3_balance - start_coin3_balance)

            if pair1_order != self.EMPTY_ORDER:
                pair1_check_order = self.client.get_order(pair1_order['id'])
                if 'message' in pair1_check_order and pair1_check_order['message'] == 'NotFound':
                    pair1_check_order = pair1_order
                print(self.PAIR1 + ' order: ', pair1_check_order)
            else:
                pair1_check_order = self.EMPTY_ORDER
                print(self.PAIR1 + ' order: None')
            if pair2_order != self.EMPTY_ORDER:
                pair2_check_order = self.client.get_order(pair2_order['id'])
                if 'message' in pair2_check_order and pair2_check_order['message'] == 'NotFound':
                    pair2_check_order = pair2_order
                print(self.PAIR2 + ' order: ', pair2_check_order)
            else:
                pair2_check_order = self.EMPTY_ORDER
                print(self.PAIR2 + ' order: None')
            if pair3_order != self.EMPTY_ORDER:
                pair3_check_order = self.client.get_order(pair3_order['id'])
                if 'message' in pair3_check_order and pair3_check_order['message'] == 'NotFound':
                    pair3_check_order = pair3_order
                print(self.PAIR3 + ' order: ', pair3_check_order)
            else:
                pair3_check_order = self.EMPTY_ORDER
                print(self.PAIR3 + ' order: None')

            # TODO: this needs to be updated now that orders/bid/ask/qty can change from
            # their initial values.  (Well, only if I change it back to update orders)
            log_list = ['kraken',order_start_time, order_end_time, self.PAIR1, self.PAIR2, self.PAIR3, final_return,
                        self.raw_order_book[self.PAIR1].bid, self.raw_order_book[self.PAIR1].ask, self.raw_order_book[self.PAIR2].bid, self.raw_order_book[self.PAIR2].ask,
                        self.raw_order_book[self.PAIR3].bid, self.raw_order_book[self.PAIR3].ask, self.FEE, self.THRESHOLD, forward_arbitrage, reverse_arbitrage,
                        base_quantity, coin2_quantity, coin3_quantity, coin1_result,
                        pair1_check_order['id'], pair1_check_order['price'], pair1_check_order['size'],
                        pair1_check_order['filled_size'], pair1_check_order['status'], pair1_check_order['side'],
                        pair2_check_order['id'], pair2_check_order['price'], pair2_check_order['size'],
                        pair2_check_order['filled_size'], pair2_check_order['status'], pair2_check_order['side'],
                        pair3_check_order['id'], pair3_check_order['price'], pair3_check_order['size'],
                        pair3_check_order['filled_size'], pair3_check_order['status'], pair3_check_order['side'],
                        start_coin1_balance, end_coin1_balance, start_coin1_value, end_coin1_value,
                        start_coin2_balance, end_coin2_balance, start_coin2_value, end_coin2_value,
                        start_coin3_balance, end_coin3_balance, start_coin3_value, end_coin3_value,
                        0.0, 0.0, 0.0,
                        0.0, 0.0, 0.0,
                        delta_coin1, delta_coin2, delta_coin3, start_total_value, end_total_value,
                        total_coin1_balance, total_coin2_balance, total_coin3_balance]
            log_string = ','.join(str(x) for x in log_list)
            print('log line: ', log_string)
            self.order_logger.info(log_string)

        # print(COIN1 + ' gain: ', end_coin1_value - start_coin1_value)
        # print(COIN2 + ' gain: ', end_coin2_value - start_coin2_value)
        # print(COIN3 + ' gain: ', end_coin3_value - start_coin3_value)
        print('total start: ', start_total_value, 'total end: ', end_total_value)
        print('total ' + self.COIN1 + ': ', total_coin1_balance, 'total ' + self.COIN2 + ': ', total_coin2_balance, 'total ' + self.COIN3 + ': ', total_coin3_balance)
        print('return: ', final_return, self.total_return)

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

        time.sleep(3)
        sys.exit(0)


    def check_logs(self):
        # restart all sockets if they've been up more than half a day
        current_time = datetime.utcnow().date()
        if current_time >= self.log_start_time + ONE_DAY:
            # starting the loggers will close down the old ones.
            self.start_logging()


    def run_arbitrage(self):
        self.start_logging()
        self.query_coin_balances()

        exception_count = 0
        while True:
            try:
                self.check_arbitrage()
                self.check_logs()
                exception_count = 0
            except Exception as e:
                self.exception_logger.error('Time: ' + datetime.utcnow().isoformat())
                self.exception_logger.error(e)
                # if e.code == -1021:
                #     self.exception_logger.info('Timestamp error, pausing and trying again')
                #     print('Timestamp error code: ', e)
                #     print('Pausing and trying again')
                #     exception_count += 1
                #     if exception_count >= 3:
                #         # this exception keeps showing up so something must be wrong.  cancel
                #         # all orders and re-raise the exception
                #         self.cancel_all_orders()
                #         raise e
                #     time.sleep(3)
                # elif e.code == -1001:
                #     self.exception_logger.error('Disconnect error, pausing and reconnecting')
                #     print('Disconnected, pause and reconnect', e)
                #     exception_count += 1
                #     if exception_count >= 3:
                #         # too many exceptions are occurring so something must be wrong.  shutdown
                #         # everything.
                #         self.cancel_all_orders()
                #         raise e
                #     self.shutdown_socket_listeners()
                #     time.sleep(3)
                #     self.launch_socket_listeners()
                # elif e.code == -2010:
                #     # insufficient funds.  this should never happen if we have accurate
                #     # values for our coin balances.  try restarting just about everything
                #     self.exception_logger.error('Time: ' + datetime.utcnow().isoformat())
                #     self.exception_logger.error('Exception placing an order, insufficient funds')
                #     self.exception_logger.error(self.COIN1 + ' Funds: ' + str(self.balance_book[self.COIN1]) + ' '
                #                                 + self.COIN2 + ' Funds: ' + str(self.balance_book[self.COIN2]) + ' '
                #                                 + self.COIN3 + ' Funds: ' + str(self.balance_book[self.COIN3]))
                #     self.exception_logger.error(e)
                #     print('Exception placing order', e)
                #     exception_count += 1
                #     if exception_count >= 5:
                #         # too many exceptions are occurring so something must be wrong.  shutdown
                #         # everything.
                #         self.cancel_all_orders()
                #         raise e
                #     self.cancel_all_orders()
                #     self.shutdown_socket_listeners()
                #     self.launch_socket_listeners()
                #     self.query_coin_balances()
                # else:
                raise e
            except requests.exceptions.ReadTimeout as e:
                self.exception_logger.error('Time: ' + datetime.utcnow().isoformat())
                self.exception_logger.error('Disconnect error, pausing and reconnecting')
                self.exception_logger.error(e)
                print('Disconnected, pause and reconnect', e)
                exception_count += 1
                if exception_count >= 3:
                    # too many exceptions are occurring so something must be wrong.  shutdown
                    # everything.
                    raise e
                time.sleep(3)
                self.client = krakenex.API()
                self.client.load_key('kraken.key')
                self.query_coin_balances()
            except Exception as e:
                print('Exitting on exception: ', e)
                self.exception_logger.error('Time: ' + datetime.utcnow().isoformat())
                self.exception_logger.error(e)
                raise e


if __name__== "__main__":
    kraken_arbitrage = KrakenArbitrage()
    kraken_arbitrage.run_arbitrage()
