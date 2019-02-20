from binance.client import Client
from binance.websockets import BinanceSocketManager
import os
import sys
import time
from binance import exceptions
import logging
from datetime import datetime, timedelta
import requests
import traceback
import gc
import copy
import json
from candlestick_chart import CandleStickChart
from binance.enums import *
from pattern_checker import BullCryptoMovingAverageChecker, BearCryptoMovingAverageChecker, PatternAction
import decimal

SAVE_STATE_FILENAME = 'bin_trend_save.json'

HALF_DAY = timedelta(hours=12)
ONE_DAY = timedelta(days=1)
ONE_WEEK = timedelta(weeks=1)
FORWARD = 1
REVERSE = 0

# binance websocket keys
START_TIME = 't'
END_TIME = 'T'
SYMBOL = 's'
INTERVAL = 'i'
OPEN = 'o'
CLOSE = 'c'
HIGH = 'h'
LOW = 'l'
VOLUME = 'v'
IS_FINISHED = 'x'
DATA = 'k'


class OrderBook:
    bid = 0
    ask = sys.maxsize


class Trade:
    time_in = None
    time_out = None
    direction = None
    price_target = 0.0
    price_in = 0.0
    price_out = 0.0
    position_size = 0.0
    risk = 0.0
    profit = 0.0
    R = 0.0
    fees = 0.0


class BinanceTrend:
    api_key = ''
    api_secret = ''

    socket_start_time = None
    log_start_time = None

    exception_logger = None
    trade_logger = None
    transaction_logger = None

    client = None
    bm = None

    btcusdt_conn_key = None
    account_conn_key = None

    # pair priorities USDT always comes last in a pair.  BTC always comes after all
    # coins other than USDT.  ETH comes after all coins other than USDT and BTC.
    # also, pair should go COIN1/COIN3, COIN2/COIN1, COIN2/COIN3
    COIN1 = 'ETH'
    COIN2 = 'USDT'
    PAIR1 = COIN1+COIN2

    TICK = dict()
    PRICE_PRECISION = dict()
    PRICE_FORMAT = dict()
    QUANTITY_PRECISION = dict()
    SPREAD_THRESHOLD = dict()
    MIN_AMOUNT = dict()
    MIN_NOTIONAL = dict()

    FEE = 0.00075
    BNB_QUANTITY = 10.0
    MIN_BNB_BALANCE = 10.0

    MAX_PERCENT_RISK = 0.01

    balance_book = {'timestamp': None,
                    'locked': False,
                    'BNB': {'free': 0.0, 'locked': 0.0},
                    'BTC': {'free': 0.0, 'locked': 0.0},
                    'ETH': {'free': 0.0, 'locked': 0.0},
                    'USDT': {'free': 0.0, 'locked': 0.0}}

    interval = Client.KLINE_INTERVAL_15MINUTE
    candlesticks = {PAIR1: CandleStickChart(PAIR1, interval)}

    total_return = 0.0
    ready_for_next = True

    def __init__(self):
        with open('api_keys.json') as api_file:
            keys = json.load(api_file)
            if 'binance' not in keys or 'api_secret' not in keys['binance'] or 'api_key' not in keys['binance']:
                print('Invalid key file {}'.format('api_keys.json'))
                exit(0)
            self.api_key = keys['binance']['api_key']
            self.api_secret = keys['binance']['api_secret']
        self.client = Client(self.api_key, self.api_secret, {'timeout': 30})
        self.bm = BinanceSocketManager(self.client)
        self.get_exchanage_data()


    def num_of_decimal_places(self, number_string):
        # return 'x' such that the input number is equivalent to 10^(-x)
        if float(number_string) >= 1.0:
            return 0
        else:
            # use str(float()) to lop off any trailing zeros
            return abs(decimal.Decimal(str(float(number_string))).as_tuple().exponent)


    def get_exchanage_data(self):
        info = self.client.get_exchange_info()
        for pair in info['symbols']:
            if pair['symbol'] in [self.PAIR1]:
                for filter in pair['filters']:
                    if filter['filterType'] == 'PRICE_FILTER':
                        self.TICK[self.PAIR1] = float(filter['tickSize'])
                        precision = self.num_of_decimal_places(filter['tickSize'])
                        self.PRICE_PRECISION[self.PAIR1] = precision
                        self.PRICE_FORMAT[self.PAIR1] = '%.{}f'.format(precision)
                    elif filter['filterType'] == 'LOT_SIZE':
                        self.MIN_AMOUNT[self.PAIR1] = float(filter['stepSize'])
                        self.QUANTITY_PRECISION[self.PAIR1] = self.num_of_decimal_places(filter['stepSize'])
                    elif filter['filterType'] == 'MIN_NOTIONAL':
                        self.MIN_NOTIONAL[self.PAIR1] = float(filter['minNotional'])


    def process_btcusdt_kline_message(self, msg):
        # add the candle to our data set
        self.candlesticks[msg[SYMBOL]].add_match(msg[SYMBOL],
                                                 msg[DATA][INTERVAL],
                                                 int(msg[DATA][START_TIME]),
                                                 float(msg[DATA][OPEN]),
                                                 float(msg[DATA][CLOSE]),
                                                 float(msg[DATA][HIGH]),
                                                 float(msg[DATA][LOW]),
                                                 float(msg[DATA][VOLUME]),
                                                 bool(msg[DATA][IS_FINISHED]))


    def process_account_message(self, msg):
        # passively update account balance
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


    def launch_socket_listeners(self):
        print('Launch socket listeners')
        self.bm = BinanceSocketManager(self.client)
        self.btcusdt_conn_key = self.bm.start_kline_socket(self.PAIR1,
                                                           self.process_btcusdt_kline_message,
                                                           interval=self.interval)
        self.account_conn_key = self.bm.start_user_socket(self.process_account_message)
        # then start the socket manager
        self.bm.start()
        self.socket_start_time = datetime.utcnow()

        print('initialize candlesticks')
        counter = 0
        while len(self.candlesticks[self.PAIR1].chart_data) == 0:
            counter += 1
            if counter > 20:
                raise Exception('Socket listener error')

            print(self.candlesticks[self.PAIR1].chart_data)
            time.sleep(1)


    def shutdown_socket_listeners(self):
        self.bm.stop_socket(self.btcusdt_conn_key)
        self.bm.stop_socket(self.account_conn_key)


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


    def start_logging(self):
        self.log_start_time = datetime.utcnow().date()

        self.trade_logger = logging.getLogger('trade_tracker')
        self.trade_logger.setLevel(logging.DEBUG)
        self.exception_logger = logging.getLogger('exception_tracker')
        self.exception_logger.setLevel(logging.DEBUG)
        self.transaction_logger = logging.getLogger('transaction_tracker')
        self.transaction_logger.setLevel(logging.DEBUG)

        base = 'logs\\'
        trade_log_template = '{dir}bin_trend_trades_{date:%Y}_{date:%m}.log'
        exception_log_template = '{dir}bin_trend_exceptions_{date:%Y}_{date:%m}.log'
        transaction_log_template = '{dir}bin_trend_transactions_{date:%Y}_{date:%m}.log'
        trade_log_file_name = trade_log_template.format(dir=base, date=self.log_start_time)
        exception_log_file_name = exception_log_template.format(dir=base, date=self.log_start_time)
        transaction_log_file_name = transaction_log_template.format(dir=base, date=self.log_start_time)
        trade_log_file_handler = logging.FileHandler(trade_log_file_name)
        trade_log_file_handler.setLevel(logging.INFO)
        exception_log_file_handler = logging.FileHandler(exception_log_file_name)
        exception_log_file_handler.setLevel(logging.INFO)
        transaction_log_file_handler = logging.FileHandler(transaction_log_file_name)
        transaction_log_file_handler.setLevel(logging.INFO)

        # remove and existing log handlers and replace them with the ones we just created
        for handler in self.trade_logger.handlers[:]:
            self.trade_logger.removeHandler(handler)
        self.trade_logger.addHandler(trade_log_file_handler)
        for handler in self.exception_logger.handlers[:]:
            self.exception_logger.removeHandler(handler)
        self.exception_logger.addHandler(exception_log_file_handler)
        for handler in self.transaction_logger.handlers[:]:
            self.transaction_logger.removeHandler(handler)
        self.transaction_logger.addHandler(transaction_log_file_handler)


    def log_transaction(self, order):
        order['exchange_name'] = 'binance'
        order_string = json.dumps(order)
        self.transaction_logger.info(order_string)


    def log_trade(self, trade):
        trade['exchange_name'] = 'binance'
        trade_json = json.dumps(trade)
        self.trade_logger.info(trade_json)


    def query_coin_balances(self):
        # actively update account balance
        result = self.client.get_account()
        self.balance_book[self.COIN1] = {'free': 0.0, 'locked': 0.0}
        self.balance_book[self.COIN2] = {'free': 0.0, 'locked': 0.0}
        self.balance_book['BNB'] = {'free': 0.0, 'locked': 0.0}
        self.balance_book['locked'] = False
        for asset in result['balances']:
            if asset['asset'] in [self.COIN1, self.COIN2, 'BNB']:
                self.balance_book[asset['asset']]['free'] += float(asset['free'])
                if float(asset['locked']) > 0:
                    self.balance_book[asset['asset']]['locked'] += float(asset['locked'])
                    self.balance_book['locked'] = True
        self.balance_book['timestamp'] = datetime.utcnow()


    def refill_bnb(self):
        if self.balance_book['BNB']['free'] < self.MIN_BNB_BALANCE:
            order = self.client.order_market_buy(symbol='BNBUSDT',
                                                 quantity=self.BNB_QUANTITY,
                                                 newOrderRespType='FULL')
            while 'status' not in order or order['status'] != 'FILLED':
                time.sleep(0.5)
                order = self.client.get_order(symbol='BNBUSDT', orderId=order['orderId'])
            order['memo'] = 'FEE_PAYMENT'
            self.log_transaction(order)
            self.log_trade(order)
            # TODO: set 'fee_price' with average fee amount


    def update_historical_candlesticks(self):
        klines = self.client.get_historical_klines(self.PAIR1, self.interval, "5 days ago UTC")
        max_historical_start_time = 0
        for candlestick in klines:
            start_time = int(candlestick[0])
            if start_time not in self.candlesticks[self.PAIR1].chart_data:
                self.candlesticks[self.PAIR1].add_match(symbol=self.PAIR1,
                                                        interval=self.interval,
                                                        start_time=int(candlestick[0]),
                                                        open=float(candlestick[1]),
                                                        high=float(candlestick[2]),
                                                        low=float(candlestick[3]),
                                                        close=float(candlestick[4]),
                                                        volume=float(candlestick[5]),
                                                        finished=True)
                max_historical_start_time = max(start_time, max_historical_start_time)
        print('count: {}'.format(len(self.candlesticks[self.PAIR1].chart_data)))

        self.candlesticks[self.PAIR1].recalc_all_metrics()
        print('metrics calculated')

        # can't enter/exit trades at historical prices so clear out any historical
        # candlesticks that are marked to be processed
        current_time = 0
        while current_time < max_historical_start_time:
            current_time = self.candlesticks[self.PAIR1].to_be_processed.get()


    def calculate_final_order_price(self, order):
        if len(order['fills']) == 0:
            return 0.0

        total_quantity = 0.0
        weighted_price = 0.0
        for trade in order['fills']:
            weighted_price += float(trade['price']) * float(trade['qty'])
            total_quantity += float(trade['qty'])
        return weighted_price / total_quantity


    def create_market_order(self, pair, direction, quantity):
        if direction not in [SIDE_BUY, SIDE_SELL]:
            raise Exception('Invalid Stop Loss direction: ({})'.format(direction))
        formatted_quantity = round(quantity, self.QUANTITY_PRECISION[self.PAIR1])
        if formatted_quantity <= 0.0:
            print('Trade quantity must be greater than 0.0: {} no trade executed {} {}'.format(quantity, pair, direction))
            return None
        try:
            market_order = self.client.create_order(symbol=pair,
                                                    side=direction,
                                                    type=ORDER_TYPE_MARKET,
                                                    quantity=formatted_quantity,
                                                    newOrderRespType='FULL')
            # need to make sure the order is filled before doing anything else
            while 'status' not in market_order or market_order['status'] != 'FILLED':
                time.sleep(0.5)
                market_order = self.client.get_order(symbol=self.PAIR1,
                                                     orderId=market_order['orderId'])
            market_order['order_type'] = 'MARKET'
            if float(market_order['price']) == 0.0:
                market_order['price'] = self.calculate_final_order_price(market_order)
            self.log_transaction(market_order)
        except exceptions.BinanceAPIException as e:
            self.exception_logger.error('Time: {}'.format(datetime.utcnow().isoformat()))
            self.exception_logger.error('Exception placing an order')
            self.exception_logger.error('Buy Pair: {} Price: Market Adjusted Quantity: {}'.format(self.PAIR1,
                                                                                                  str(formatted_quantity)))
            if e.code == -1013:
                print('Value under min notional: {} {} {}'.format(self.MIN_NOTIONAL[self.PAIR1],
                                                                  direction,
                                                                  quantity))
            self.exception_logger.error(traceback.format_exc())
            print('Exception placing order', e)
            raise e
        return market_order


    def create_stop_loss_limit_trade(self, pair, direction, stop_price, sale_price, quantity):
        if direction not in [SIDE_BUY, SIDE_SELL]:
            raise Exception('Invalid Stop Loss direction: ({})'.format(direction))
        formatted_stop_price = self.PRICE_FORMAT[self.PAIR1] % stop_price
        formatted_price = self.PRICE_FORMAT[self.PAIR1] % sale_price
        formatted_quantity = round(quantity, self.QUANTITY_PRECISION[self.PAIR1])
        try:
            stop_order = self.client.create_order(symbol=pair,
                                                  side=direction,
                                                  type=ORDER_TYPE_STOP_LOSS_LIMIT,
                                                  stopPrice=formatted_stop_price,
                                                  price=formatted_price,
                                                  timeInForce=TIME_IN_FORCE_GTC,
                                                  quantity=formatted_quantity)
            stop_order['order_type'] = 'STOP_LOSS_LIMIT'
            stop_order['stop_price'] = formatted_stop_price
            stop_order['price'] = formatted_price
            stop_order['quantity'] = formatted_quantity
            self.log_transaction(stop_order)
        except exceptions.BinanceAPIException as e:
            self.exception_logger.error('Time: {}'.format(datetime.utcnow().isoformat()))
            self.exception_logger.error('Exception placing a stop loss {} order'.format(direction))
            self.exception_logger.error('{} Pair: {} Adjusted Quantity: {}'.format(direction,
                                                                                   self.PAIR1,
                                                                                   str(formatted_quantity)))
            self.exception_logger.error('Stop price: {} Order price: {}'.format(formatted_stop_price,
                                                                                formatted_price))
            if e.code == -1013:
                print('Value under min notional: {}'.format(self.MIN_NOTIONAL[self.PAIR1]))
            self.exception_logger.error(traceback.format_exc())
            print('Exception placing order', e)
            raise e
        return stop_order


    def find_trade_for_order(self, order):
        trade_list = []
        print('looking for trades for order: {}', order)
        if 'time' in order:
            start = order['time']
        elif 'transactTime' in order:
            start = order['transactTime']
        elif 'updateTime' in order:
            start = order['updateTime']
        else:
            raise Exception('find_trade_for_order: Order does not have a start time: {}'.format(order))

        # Sometimes we get read timeouts looking up trades.  Try a few times before giving up.
        for i in range(0, 10):
            try:
                trade_list = self.client.get_my_trades(symbol=order['symbol'], startTime=start)
                break
            except requests.exceptions.ReadTimeout as e:
                self.exception_logger.warning('Time: ' + datetime.utcnow().isoformat())
                self.exception_logger.warning('Read timeout looking up trades. {}'.format(e))
                self.exception_logger.warning(traceback.format_exc())
                pass
        print('trade list: {}'.format(trade_list))
        order_trades = []
        for trade in trade_list:
            if trade['orderId'] == order['orderId']:
                print('found a good trade: {}'.format(trade))
                order_trades.append(trade)
            else:
                print('not part of order ({}): {}'.format(order['orderId'], trade))
        return order_trades


    def cancel_order(self, order):
        print('Cancelling: {}'.format(order['symbol']))
        try:
            self.client.cancel_order(symbol=order['symbol'], orderId=order['orderId'])
        except exceptions.BinanceAPIException as e:
            if e.message == 'UNKNOWN_ORDER' or e.code == -2011:
                print('Order already filled')
            else:
                self.exception_logger.error('Time: ' + datetime.utcnow().isoformat())
                self.exception_logger.error(traceback.format_exc())
                time.sleep(3)
                raise e


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
                    time.sleep(3)
                    raise e


    def update_fees(self, trade, order_type):
        # calculate entry fees
        for fill in trade[order_type]['fills']:
            if fill['commissionAsset'] in trade['fee']:
                trade['fee'][fill['commissionAsset']] += float(fill['commission'])
            else:
                trade['fee'][fill['commissionAsset']] = float(fill['commission'])


    def set_save_point(self, trade, trade_status, pattern_checker_name, pattern_checker_stop_loss, pattern_checker_status):
        print('saving to file: {}'.format(SAVE_STATE_FILENAME))
        save_point = dict()
        save_point['trade'] = trade
        save_point['status'] = trade_status
        save_point['pattern_checker_name'] = pattern_checker_name
        save_point['pattern_checker_stop_loss'] = pattern_checker_stop_loss
        save_point['pattern_checker_status'] = pattern_checker_status
        save_point['exchange'] = 'binance'
        save_point['interval'] = self.interval
        save_point['pair'] = self.PAIR1
        save_point['timestamp'] = datetime.utcnow().isoformat()

        with open(SAVE_STATE_FILENAME, 'w') as save_file:
            save_file.write(json.dumps({'bin_trend_save_state': save_point}))
            print('data saved: {}'.format(save_point))


    def clear_save_point(self):
        print('removing save point')
        os.remove(SAVE_STATE_FILENAME)


    def read_save_point(self):
        # read in the save file
        try:
            with open(SAVE_STATE_FILENAME, 'r') as save_file:
                previous_state = save_file.read()
                if previous_state != '':
                    # parse out values
                    save_point = json.loads(previous_state)['bin_trend_save_state']

                    # see if the save file is still valid.

                    # verify the exchange
                    if save_point['exchange'] != 'binance':
                        return None
                    # verify the interval
                    if save_point['interval'] != self.interval:
                        return None
                    # verify the trading pair
                    if save_point['pair'] != self.PAIR1:
                        return None

                    # check that the stop loss is still in place.  if not, the save point is no longer valid

                    # check that the last trade for this pair is the one in the save point

                    # restore the save point
                    return save_point
        except FileNotFoundError:
            pass

        return None

    def check_trend(self):
        start_time = datetime.utcnow()
        #time.sleep(30)

        # grab all historical data to initialize metrics
        self.update_historical_candlesticks()

        chart = self.candlesticks[self.PAIR1]
        checker_list = dict()
        checker_list['BullCryptoMA'] = BullCryptoMovingAverageChecker(chart.chart_data)
        checker_list['BearCryptoMA'] = BearCryptoMovingAverageChecker(chart.chart_data)
        current_status = PatternAction.WAIT
        current_checker = None
        checker_name = None
        trade_list = {}
        current_trade = None
        win_count = 0
        win_total = 0
        win_profit = 0
        loss_count = 0
        loss_total = 0
        loss_cost = 0

        save_point = self.read_save_point()
        if save_point:
            print('Restoring from save point')
            current_trade = save_point['trade']
            current_status = save_point['status']
            print('Status: {}'.format(current_status))
            checker_name = save_point['pattern_checker_name']
            print('Pattern: {}'.format(checker_name))
            current_checker = checker_list[checker_name]
            current_checker.stop_loss = save_point['pattern_checker_stop_loss']
            current_checker.status = save_point['pattern_checker_status']

            if current_trade:
                log_template = '{timestamp} {direction} price in: {price_in} size: {position} risk: {risk} max retrace: {retrace}'
                print(log_template.format(direction=current_trade['direction'],
                                          price_in=current_trade['price_in'],
                                          risk=current_trade['risk'],
                                          position=current_trade['position_size'],
                                          timestamp=current_trade['time_in'],
                                          retrace=current_trade['max_retracement']))
                # make sure the stop loss is still in place
                stop_order = self.client.get_order(symbol=self.PAIR1,
                                                   orderId=current_trade['stop_loss_order']['orderId'])
                print('stop order: {}'.format(stop_order))
                if 'status' not in stop_order or stop_order['status'] == 'CANCELED':
                    # re-create the stop order if it doesn't exist or was canceled
                    print('Stop loss order was canceled.  Create a new one.')
                    stop_order = self.create_stop_loss_limit_trade(pair=self.PAIR1,
                                                                   direction=stop_order['side'],
                                                                   stop_price=float(stop_order['stopPrice']),
                                                                   sale_price=float(stop_order['price']),
                                                                   quantity=float(stop_order['origQty']))
                    current_trade['stop_loss_order'] = stop_order
                    # since we re-created the stop loss order, we also need to re-create the save point
                    self.set_save_point(trade=current_trade,
                                        trade_status=current_status,
                                        pattern_checker_name=checker_name,
                                        pattern_checker_stop_loss=current_checker.stop_loss,
                                        pattern_checker_status=current_checker.status)
                elif stop_order['status'] == 'FILLED':
                    # we hit the stop loss while offline.  reset trade
                    print('Stop loss was filled while offline.  Start fresh.')

                    # TODO Clean this up

                    current_trade['price_out_target'] = stop_order['stopPrice']
                    current_trade['stop_loss_order'] = stop_order
                    exit_order = stop_order
                    stop_order['original_price'] = stop_order['price']

                    if 'fills' not in exit_order:
                        exit_order['fills'] = self.find_trade_for_order(exit_order)
                        if len(exit_order['fills']) > 0:
                            exit_order['price'] = self.calculate_final_order_price(exit_order)
                        else:
                            self.exception_logger.error('Time: {}'.format(datetime.utcnow().isoformat()))
                            self.exception_logger.warning('Unable to find trades for order: {}'.format(exit_order))
                            exit_order['price'] = 0.0
                    self.log_transaction(stop_order)

                    current_trade['exit_order'] = exit_order
                    current_trade['price_out'] = float(exit_order['price'])
                    if current_trade['direction'] == PatternAction.GO_LONG:
                        current_trade['profit'] = (current_trade['price_out'] - current_trade['price_in']) * current_trade['position_size']
                    else:
                        current_trade['profit'] = (current_trade['price_in'] - current_trade['price_out']) * current_trade['position_size']
                    # calculate exit fees
                    self.update_fees(current_trade, 'exit_order')

                    current_trade['R'] = current_trade['profit'] / current_trade['risk']
                    if 'updateTime' in stop_order:
                        current_trade['time_out'] = datetime.utcfromtimestamp(stop_order['updateTime']/1000.0).isoformat()
                    elif 'transactTime' in stop_order:
                        current_trade['time_out'] = datetime.utcfromtimestamp(
                            stop_order['transactTime'] / 1000.0).isoformat()
                    elif 'time' in stop_order:
                        current_trade['time_out'] = datetime.utcfromtimestamp(
                            stop_order['time'] / 1000.0).isoformat()
                    else:
                        current_trade['time_out'] = datetime.utcnow().isoformat()
                    self.query_coin_balances()
                    current_trade['balance_out'] = {self.COIN1: self.balance_book[self.COIN1]['free'],
                                                    self.COIN2: self.balance_book[self.COIN2]['free']}
                    log_template = '{direction} {price_in} {price_out} profit: {profit} risk: {risk} reward: {reward}'
                    print(log_template.format(direction=current_trade['direction'],
                                              price_in=current_trade['price_in'],
                                              price_out=current_trade['price_out'],
                                              profit=current_trade['profit'],
                                              risk=current_trade['risk'],
                                              reward=current_trade['R']))
                    self.log_trade(current_trade)
                    current_checker.stop_loss = 0
                    current_checker.status = PatternAction.WAIT
                    self.clear_save_point()

                    current_trade = None
                    current_status = PatternAction.WAIT
                    checker_name = None
                    current_checker = None
        else:
            print('No save point')


        while True:
            if not chart.metric_to_be_processed.empty():
                chart.update_metrics()
                self.refill_bnb()
            if chart.to_be_processed.empty():
                time.sleep(5)
                continue
            time_stamp = chart.to_be_processed.get()
            print('timestamp is ready: {} {}'.format(datetime.utcfromtimestamp(time_stamp / 1000).isoformat(),
                                                     chart.to_be_processed.qsize()))

            #### TODO Set up for using WAIT->ENTER->HOLD->EXIT->WAIT
            # determine what action we want to take next, if any
            proposed_action = None
            position = None
            if current_status in [PatternAction.WAIT]:
                # we are currently not in a trade, see if we should go long or short or do nothing
                for name, checker in checker_list.items():
                    print('check for entry {}'.format(name))
                    proposed_action, position = checker.check_entry(time_stamp)
                    if proposed_action in [PatternAction.GO_LONG, PatternAction.GO_SHORT]:
                        # go with the first algo that gives us a signal (initially, the two are mutually
                        # exclusive, will need to update when they overlap)
                        current_checker = checker
                        checker_name = name
                        break
            elif current_status in [PatternAction.HOLD]:
                print('check for exit: {:.2f} {:.4f} {:.4f} {:.4f}'.format(chart.chart_data[time_stamp].close,
                                                                           chart.chart_data[time_stamp].metric['ema200'],
                                                                           current_trade['price_in'],
                                                                           current_checker.stop_loss))
                # we are currently in a trade, see if we should exit
                proposed_action, position = current_checker.check_exit(time_stamp)

            if proposed_action in [PatternAction.GO_LONG, PatternAction.GO_SHORT]:
                # we found an opportunity, enter a trade
                print('{time} {action} {checker}'.format(time=datetime.utcfromtimestamp(time_stamp / 1000).isoformat(),
                                                         action=proposed_action,
                                                         checker=checker_name))
                current_trade = self.enter_trade(direction=proposed_action,
                                                 price_in=chart.chart_data[time_stamp].close,
                                                 stop_loss=current_checker.stop_loss,
                                                 max_risk=self.MAX_PERCENT_RISK * self.balance_book[self.COIN2]['free'])
                current_trade['pattern'] = checker_name
                current_status = PatternAction.HOLD
                current_checker.status = PatternAction.HOLD
                # save our current state so we can restore if something fails.
                self.set_save_point(trade=current_trade,
                                    trade_status=current_status,
                                    pattern_checker_name=checker_name,
                                    pattern_checker_stop_loss=current_checker.stop_loss,
                                    pattern_checker_status=current_checker.status)
                trade_list[time_stamp] = current_trade

            elif proposed_action == PatternAction.EXIT_TRADE:
                # time to exit
                print('{time} {action} {checker}'.format(time=datetime.utcfromtimestamp(time_stamp / 1000).isoformat(),
                                                         action=proposed_action,
                                                         checker=checker_name))
                self.exit_trade(current_trade, price_out=chart.chart_data[time_stamp].close)
                current_trade['price_out_threshold'] = position
                current_status = PatternAction.WAIT
                current_checker.status = PatternAction.WAIT
                # we're out of the trade now so we can clear the save point
                self.clear_save_point()
                # update statistics
                if current_trade['profit'] > 0:
                    win_count += 1
                    win_total += current_trade['R']
                    win_profit += current_trade['profit']
                else:
                    loss_count += 1
                    loss_total += current_trade['R']
                    loss_cost += current_trade['profit']
                self.log_trade(current_trade)
                print('Wins {} = {} ({}) Losses {} = {} ({})'.format(win_count, win_total, win_profit, loss_count,
                                                                     loss_total, loss_cost))
        # exit the last trade early if we're still in the market
        # if current_checker.status == PatternAction.HOLD:
        #     print('{} {}'.format(datetime.utcfromtimestamp(time_stamp_list[-1] / 1000).isoformat(), 'Forced Exit'))
        #     self.exit_trade(current_trade, price_out=self.candlesticks[self.PAIR1].chart_data[time_stamp_list[-1]].close)
        #     if current_trade['profit'] > 0:
        #         win_count += 1
        #         win_total += current_trade['R']
        #         win_profit += current_trade['profit']
        #     else:
        #         loss_count += 1
        #         loss_total += current_trade['R']
        #         loss_cost += current_trade['profit']

        print('Wins {} = {} ({}) Losses {} = {} ({})'.format(win_count, win_total, win_profit, loss_count, loss_total, loss_cost))
        end_time = datetime.utcnow()
        print('Runtime: {}'.format(end_time-start_time))
        exit(0)
        pass


    def enter_trade(self, direction, price_in, stop_loss, max_risk):
        max_risk = self.MAX_PERCENT_RISK * (self.balance_book[self.COIN2]['free'] +
                                            self.balance_book[self.COIN1]['free'] * price_in)
        max_retracement = abs(price_in - stop_loss)
        position_size = max_risk / max_retracement

        self.query_coin_balances()

        if direction == PatternAction.GO_LONG:
            current_balance = self.balance_book[self.COIN2]['free']
            if position_size * price_in < 0.9 * current_balance:
                actual_risk = max_risk
            else:
                # balance too small to fund at max risk.
                position_size = (0.9 * current_balance) / price_in
                actual_risk = position_size * max_retracement
            side = SIDE_BUY
            stop_loss_side = SIDE_SELL
            stop_loss_sale_price = stop_loss * 0.75
            # verify stop loss is less than price in
        elif direction == PatternAction.GO_SHORT:
            current_balance = self.balance_book[self.COIN1]['free']
            if position_size < 0.9 * current_balance:
                actual_risk = max_risk
            else:
                # balance too small to fund at max risk.
                position_size = 0.9 * current_balance
                actual_risk = position_size * max_retracement
            side = SIDE_SELL
            stop_loss_side = SIDE_BUY
            stop_loss_sale_price = stop_loss * 1.25
            # verify stop loss is more than price in
        else:
            # Shouldn't happen
            return
        print('current balance: {}'.format(current_balance))

        trade = {'time_in': datetime.utcnow().isoformat(),
                 'direction': direction,
                 'price_in_target': price_in,
                 'position_size': round(position_size, self.QUANTITY_PRECISION[self.PAIR1]),
                 'risk': actual_risk,
                 'max_risk': max_risk,
                 'fee': {},
                 'max_retracement': max_retracement,
                 'balance_in': {self.COIN1: self.balance_book[self.COIN1]['free'],
                                self.COIN2: self.balance_book[self.COIN2]['free'],
                                'BNB': self.balance_book['BNB']['free']}}
        log_template = '{direction} price in: {price_in} max retrace: {retrace} size: {position} risk: {risk}'
        print(log_template.format(direction=direction,
                                  price_in=price_in,
                                  risk=actual_risk,
                                  position=position_size,
                                  retrace=max_retracement))

        # execute market order buy with a stop loss
        print('adjusted {}'.format(trade['position_size']))
        entry_order = self.create_market_order(pair=self.PAIR1,
                                               direction=side,
                                               quantity=trade['position_size'])
        stop_order = self.create_stop_loss_limit_trade(pair=self.PAIR1,
                                                       direction=stop_loss_side,
                                                       stop_price=stop_loss,
                                                       sale_price=stop_loss_sale_price,
                                                       quantity=trade['position_size'])
        print('new long order: {}'.format(entry_order))
        print('new stop order: {}'.format(stop_order))

        trade['entry_order'] = entry_order
        trade['stop_loss_order'] = stop_order

        # log actual price of purchase
        trade['price_in'] = float(entry_order['price'])

        # calculate entry fees
        self.update_fees(trade, 'entry_order')

        return trade


    def exit_trade(self, trade, price_out):
        # check stop order to see if we already exited the trade
        stop_order = self.client.get_order(symbol=self.PAIR1,
                                           orderId=trade['stop_loss_order']['orderId'])
        if stop_order['status'] == 'FILLED':
            # Hit a hard stop, nothing to do but record the data
            exit_order = stop_order
            stop_order['original_price'] = stop_order['price']
            trade['price_out_target'] = stop_order['stopPrice']
            trade['stop_loss_order'] = stop_order
            if 'updateTime' in stop_order:
                trade['time_out'] = datetime.utcfromtimestamp(stop_order['updateTime'] / 1000.0).isoformat()
            elif 'transactTime' in stop_order:
                trade['time_out'] = datetime.utcfromtimestamp(stop_order['transactTime'] / 1000.0).isoformat()
            elif 'time' in stop_order:
                trade['time_out'] = datetime.utcfromtimestamp(stop_order['time'] / 1000.0).isoformat()
            else:
                trade['time_out'] = datetime.utcnow().isoformat()

        else:
            trade['price_out_target'] = price_out
            # the stop loss order wasn't executed, cancel it and exit the trade
            # TODO: put this in a try/except block on the off chance we cancel it right after
            # TODO:    it gets filled.
            stop_order = self.client.cancel_order(symbol=self.PAIR1, orderId=stop_order['orderId'])

            # shrink/grow the position size so in the end we have approximately equal valued
            # positions in coin1 and USDT.
            coin1_balance = self.balance_book[self.COIN1]['free'] + self.balance_book[self.COIN1]['locked']
            coin2_balance = self.balance_book[self.COIN2]['free'] + self.balance_book[self.COIN2]['locked']
            print('Pre-exit balances: {} {}/{} {} {}/{}'.format(self.COIN1,
                                                                self.balance_book[self.COIN1]['free'],
                                                                self.balance_book[self.COIN1]['locked'],
                                                                self.COIN2,
                                                                self.balance_book[self.COIN2]['free'],
                                                                self.balance_book[self.COIN2]['locked']))
            position_mid = 0.5 * (coin1_balance + (coin2_balance / price_out))
            if trade['direction'] == PatternAction.GO_LONG:
                direction = SIDE_SELL
                position_out = coin1_balance - position_mid
            else:
                direction = SIDE_BUY
                position_out = position_mid - coin1_balance

            if position_out > trade['position_size']:
                # don't buy/sell more than we originally did, only less
                position_out = trade['position_size']

            print('exit size: {} price: {} {}'.format(position_out, price_out, direction))
            # if position_out is negative, this will return None
            exit_order = self.create_market_order(pair=self.PAIR1,
                                                  direction=direction,
                                                  quantity=position_out)
            trade['time_out'] = datetime.utcnow().isoformat()

        if exit_order and 'fills' not in exit_order:
            exit_order['fills'] = self.find_trade_for_order(exit_order)
            print('fills: {}'.format(exit_order['fills']))
            if len(exit_order['fills']) > 0:
                exit_order['price'] = self.calculate_final_order_price(exit_order)
            else:
                self.exception_logger.error('Time: {}'.format(datetime.utcnow().isoformat()))
                self.exception_logger.warning('Unable to find trades for order: {}'.format(exit_order))
                exit_order['price'] = 0.0
        if stop_order['status'] == 'FILLED':
            # we need to wait to log the stop order till after we look for the transactions that
            # filled the order
            self.log_transaction(stop_order)
        print('new exit order: {}'.format(exit_order))
        print('canceled stop order: {}'.format(stop_order))
        trade['exit_order'] = exit_order
        trade['stop_loss_order'] = stop_order  # TODO make sure the stop order is canceled
        if exit_order and float(exit_order['price'] > 0):
            trade['price_out'] = float(exit_order['price'])
        else:
            # we sold the position "virtually" so just use the target price_out
            trade['price_out'] = price_out
        if trade['direction'] == PatternAction.GO_LONG:
            trade['profit'] = (trade['price_out'] - trade['price_in']) * trade['position_size']
        else:
            trade['profit'] = (trade['price_in'] - trade['price_out']) * trade['position_size']
        # calculate exit fees
        self.update_fees(trade, 'exit_order')

        trade['R'] = trade['profit'] / trade['risk']
        self.query_coin_balances()
        trade['balance_out'] = {self.COIN1: self.balance_book[self.COIN1]['free'],
                                self.COIN2: self.balance_book[self.COIN2]['free'],
                                'BNB': self.balance_book['BNB']['free']}
        log_template = '{direction} {price_in} {price_out} profit: {profit} risk: {risk} reward: {reward}'
        print(log_template.format(direction=trade['direction'],
                                  price_in=trade['price_in'],
                                  price_out=trade['price_out'],
                                  profit=trade['profit'],
                                  risk=trade['risk'],
                                  reward=trade['R']))
        print('Post-exit balances: {} {}/{} {} {}/{}'.format(self.COIN1,
                                                             self.balance_book[self.COIN1]['free'],
                                                             self.balance_book[self.COIN1]['locked'],
                                                             self.COIN2,
                                                             self.balance_book[self.COIN2]['free'],
                                                             self.balance_book[self.COIN2]['locked']))

    def quick_exit(self):
        save_point = self.read_save_point()
        if save_point:
            print('Reading save point')
            current_trade = save_point['trade']

            if current_trade:
                log_template = '{timestamp} {direction} price in: {price_in} size: {position} risk: {risk}'
                print(log_template.format(direction=current_trade['direction'],
                                          price_in=current_trade['price_in'],
                                          risk=current_trade['risk'],
                                          position=current_trade['position_size'],
                                          timestamp=current_trade['time_in']))
                print('Exiting trade.')
                # time to exit
                self.exit_trade(current_trade, price_out=current_trade['price_in'])
                # we're out of the trade now so we can clear the save point
                self.clear_save_point()
                self.log_trade(current_trade)


    def run_trend(self):

            for i in range(1, 5):
                local_time1 = int(time.time() * 1000)
                server_time = self.client.get_server_time()
                local_time2 = int(time.time() * 1000)
                diff1 = server_time['serverTime'] - local_time1
                diff2 = local_time2 - server_time['serverTime']
                print("local1: %s server:%s local2: %s diff1:%s diff2:%s" % (local_time1,
                                                                             server_time['serverTime'],
                                                                             local_time2,
                                                                             diff1,
                                                                             diff2))
                time.sleep(2)


            self.start_logging()
            #self.cancel_all_orders()
            self.query_coin_balances()
            self.launch_socket_listeners()

            exception_count = 0
#        while True:
            try:
                self.check_trend()
                self.check_sockets()
                self.check_logs()
                exception_count = 0
            except exceptions.BinanceAPIException as e:
                self.exception_logger.error('Time: {}'.format(datetime.utcnow().isoformat()))
                self.exception_logger.error(traceback.format_exc())
                if e.code == -1021:
                    self.exception_logger.info('Timestamp error, pausing and trying again')
                    print('Timestamp error code: ', e)
                    print('Pausing and trying again')
                    exception_count += 1
                    # if exception_count >= 3:
                        # this exception keeps showing up so something must be wrong.  cancel
                        # all orders and re-raise the exception
                        # self.cancel_all_orders()
                        # raise e
                    time.sleep(3)
                elif e.code == -1001:
                    self.exception_logger.error('Disconnect error, pausing and reconnecting')
                    print('Disconnected, pause and reconnect', e)
                    exception_count += 1
                    # if exception_count >= 3:
                        # too many exceptions are occurring so something must be wrong.  shutdown
                        # everything.
                        # self.cancel_all_orders()
                        # raise e
                    self.shutdown_socket_listeners()
                    time.sleep(3)
                    self.launch_socket_listeners()
                elif e.code == -2010:
                    # insufficient funds.  this should never happen if we have accurate
                    # values for our coin balances.  try restarting just about everything
                    self.exception_logger.error('Time: {}'.format(datetime.utcnow().isoformat()))
                    self.exception_logger.error('Exception placing an order, insufficient funds')
                    self.exception_logger.error('{} Funds: {} {}'.format(self.COIN1,
                                                                         str(self.balance_book[self.COIN1]['free']),
                                                                         str(self.balance_book[self.COIN1]['locked'])))
                    self.exception_logger.error('{} Funds: {} {}'.format(self.COIN2,
                                                                         str(self.balance_book[self.COIN2]['free']),
                                                                         str(self.balance_book[self.COIN1]['locked'])))
                    self.exception_logger.error(traceback.format_exc())
                    print('Exception placing order', e)
                    exception_count += 1
                    # if exception_count >= 5:
                    #     # too many exceptions are occurring so something must be wrong.  shutdown
                    #     # everything.
                    #     self.cancel_all_orders()
                    #     raise e
                    # self.cancel_all_orders()
                    self.shutdown_socket_listeners()
                    time.sleep(3)
                    self.launch_socket_listeners()
                    self.query_coin_balances()
                else:
                    time.sleep(3)
                    raise e
            except requests.exceptions.ReadTimeout as e:
                self.exception_logger.error('Time: {}'.format(datetime.utcnow().isoformat()))
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
                # self.cancel_all_orders()
                self.query_coin_balances()
                self.launch_socket_listeners()
            except Exception as e:
                print('Exitting on exception: ', e)
                self.exception_logger.error('Time: {}'.format(datetime.utcnow().isoformat()))
                self.exception_logger.error(traceback.format_exc())
                self.shutdown_socket_listeners()
                # raise e
                time.sleep(3)
                self.client = Client(self.api_key, self.api_secret)
                self.bm = BinanceSocketManager(self.client)
                # self.cancel_all_orders()
                self.query_coin_balances()
                self.launch_socket_listeners()


if __name__ == "__main__":
        exception_count = 0
    # while True:
        try:
            start_time = datetime.utcnow()
            binance_trend = BinanceTrend()
            binance_trend.run_trend()
        except Exception as e:
            print('Failure at the top level', str(e))
            traceback.print_exc()
            exception_time = datetime.utcnow()
            if exception_time - start_time < timedelta(minutes=30):
                if exception_count > 3:
                    time.sleep(3)
                    raise e
                else:
                    exception_count += 1
            else:
                exception_count = 0
            binance_trend = None
            time.sleep(60)
            gc.collect()
            time.sleep(60)


