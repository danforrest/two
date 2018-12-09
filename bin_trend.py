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
import json
from candlestick_chart import CandleStickChart
from binance.enums import *
from pattern_checker import BullCryptoMovingAverageChecker, BearCryptoMovingAverageChecker, PatternAction

HALF_DAY = timedelta(hours=12)
ONE_DAY = timedelta(days=1)
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
    COIN1 = 'BTC'
    COIN2 = 'USDT'
    PAIR1 = 'BTCUSDT'

    TICK = {'BTCUSDT': 0.01}
    PRICE_PRECISION = {'BTCUSDT': 2}
    PRICE_FORMAT = {'BTCUSDT': '%.2f'}
    QUANTITY_PRECISION = {'BTCUSDT': 6}
    SPREAD_THRESHOLD = {'BTCUSDT': 0.75}
    MIN_AMOUNT = {'BTCUSDT': 0.000001}
    MIN_NOTIONAL = {'BTCUSDT': 1.0}

    FEE = 0.00075
    BNB_QUANTITY = 10.0
    MIN_BNB_BALANCE = 10.0

    balance_book = {'timestamp': None,
                    'locked': False,
                    'BNB': {'free': 0.0, 'locked': 0.0},
                    'BTC': {'free': 0.0, 'locked': 0.0},
                    'USDT': {'free': 0.0, 'locked': 0.0}}

    interval = Client.KLINE_INTERVAL_1MINUTE
    candlesticks = {'BTCUSDT': CandleStickChart('BTCUSDT', interval)}

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
        self.client = Client(self.api_key, self.api_secret)
        self.bm = BinanceSocketManager(self.client)

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
        self.btcusdt_conn_key = self.bm.start_kline_socket('BTCUSDT',
                                                           self.process_btcusdt_kline_message,
                                                           interval=self.interval)
        self.account_conn_key = self.bm.start_user_socket(self.process_account_message)
        # then start the socket manager
        self.bm.start()
        self.socket_start_time = datetime.utcnow()

        print('initialize candlesticks')
        counter = 0
        while len(self.candlesticks['BTCUSDT'].chart_data) == 0:
            counter += 1
            if counter > 20:
                raise Exception('Socket listener error')

            print(self.candlesticks['BTCUSDT'].chart_data)
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
        trade_log_file_name = '%sbin_trend_trades_%s.log' % (base, self.log_start_time.isoformat())
        exception_log_file_name = '%sbin_trend_exceptions_%s.log' % (base, self.log_start_time.isoformat())
        transaction_log_file_name = '%sbin_trend_transactions_%s.log' % (base, self.log_start_time.isoformat())
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
        trade['exchange'] = 'binance'
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
        total_quantity = 0.0
        weighted_price = 0.0
        for trade in order['fills']:
            weighted_price += trade['price'] * trade['qty']
            total_quantity += trade['qty']
        return weighted_price / total_quantity


    def create_market_order(self, pair, direction, quantity):
        if direction not in [SIDE_BUY, SIDE_SELL]:
            raise Exception('Invalid Stop Loss direction: ({})'.format(direction))
        formatted_quantity = round(quantity, self.QUANTITY_PRECISION[self.PAIR1])
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
                print('Value under min notional: {}'.format(self.MIN_NOTIONAL[self.PAIR1]))
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
            self.exception_logger.error('Exception placing a stop loss sell order')
            self.exception_logger.error('Sell Pair: {} Adjusted Quantity: {}'.format(self.PAIR1,
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
        print('looking for trades for order: {}', order)
        trade_list = self.client.get_my_trades(symbol=order['symbol'],
                                               startTime=order['transactTime'])
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
        if 'fills' not in trade[order_type]:
            trade[order_type]['fills'] = self.find_trade_for_order(trade[order_type])
            print('{} fills: {}'.format(order_type, trade[order_type]['fills']))
        for fill in trade[order_type]['fills']:
            trade['fee'][fill['commissionAsset']] = fill['commission']


    def check_trend(self):
        start_time = datetime.utcnow()
        #time.sleep(30)

        # grab all historical data to initialize metrics
        self.update_historical_candlesticks()

        chart = self.candlesticks[self.PAIR1]
        checker_list = {}
        checker_list['BullCryptoMA'] = BullCryptoMovingAverageChecker(chart.chart_data)
        checker_list['BearCryptoMA'] = BearCryptoMovingAverageChecker(chart.chart_data)
        current_status = PatternAction.WAIT
        current_checker = None
        checker_name = None
        trade_list = {}
        current_trade = None
        MAX_RISK = 2
        win_count = 0
        win_total = 0
        win_profit = 0
        loss_count = 0
        loss_total = 0
        loss_cost = 0
        while True:
            if not chart.metric_to_be_processed.empty():
                chart.update_metrics()
                self.refill_bnb()
            if chart.to_be_processed.empty():
                time.sleep(5)
                continue
            time_stamp = chart.to_be_processed.get()
            print('timestamp is ready: {} {}'.format(time_stamp, chart.to_be_processed.qsize()))

            #### TODO Set up for using WAIT->ENTER->HOLD->EXIT->WAIT
            # determine what action we want to take next, if any
            proposed_action = None
            position = None
            if current_status in [PatternAction.WAIT]:
                print('check for entry')
                # we are currently not in a trade, see if we should go long or short or do nothing
                for name, checker in checker_list.items():
                    proposed_action, position = checker.check_entry(time_stamp)
                    if proposed_action in [PatternAction.GO_LONG, PatternAction.GO_SHORT]:
                        # go with the first algo that gives us a signal (initially, the two are mutually
                        # exclusive, will need to update when they overlap)
                        current_checker = checker
                        checker_name = name
                        break
            elif current_status in [PatternAction.HOLD]:
                print('check for exit')
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
                                                 max_risk=MAX_RISK)
                current_trade['pattern'] = checker_name
                current_status = PatternAction.HOLD
                current_checker.status = PatternAction.HOLD
                trade_list[time_stamp] = current_trade

            elif proposed_action == PatternAction.EXIT_TRADE:
                # time to exit
                print('{time} {action} {checker}'.format(time=datetime.utcfromtimestamp(time_stamp / 1000).isoformat(),
                                                         action=proposed_action,
                                                         checker=checker_name))
                self.exit_trade(current_trade, price_out=position)
                current_status = PatternAction.WAIT
                current_checker.status = PatternAction.WAIT
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
        max_retracement = abs(price_in - stop_loss)
        position_size = max_risk / max_retracement

        if direction == PatternAction.GO_LONG:
            current_balance = self.balance_book['USDT']['free']
            if position_size * price_in < 0.9 * current_balance:
                actual_risk = max_risk
            else:
                # balance too small to fund at max risk.
                position_size = (0.9 * current_balance) / price_in
                actual_risk = position_size * max_retracement
            side = SIDE_BUY
            stop_loss_side = SIDE_SELL
            stop_loss_sale_price = stop_loss * 0.5
            # verify stop loss is less than price in
        elif direction == PatternAction.GO_SHORT:
            current_balance = self.balance_book['BTC']['free']
            if position_size < 0.9 * current_balance:
                actual_risk = max_risk
            else:
                # balance too small to fund at max risk.
                position_size = 0.9 * current_balance
                actual_risk = position_size * max_retracement
            side = SIDE_SELL
            stop_loss_side = SIDE_BUY
            stop_loss_sale_price = stop_loss * 2.0
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
                 'fee': {}}
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
        trade['price_in'] = entry_order['price']  # TODO: verify that price is weighted by quantity at each price

        # calculate entry fees
        self.update_fees(trade, 'entry_order')

        return trade


    def exit_trade(self, trade, price_out=0.0):
        if trade['direction'] == PatternAction.GO_LONG:
            direction = SIDE_SELL
        else:
            direction = SIDE_BUY

        trade['price_out_target'] = price_out
        # check stop order to see if we already exited the trade
        stop_order = self.client.get_order(symbol=self.PAIR1,
                                           orderId=trade['stop_loss_order']['orderId'])
        if stop_order['status'] == 'FILLED':
            # Hit a hard stop, nothing to do but record the data
            trade['stop_loss_order'] = stop_order
            exit_order = stop_order
            # trade['price_out'] = float(stop_order['price'])  # TODO: see if this is average / best / target
            # trade['profit'] = (trade['price_out'] - trade['price_in']) * trade['position_size']
            # # calculate stop loss fees
            # self.update_fees(trade, 'stop_loss_order')
        else:
            # the stop loss order wasn't executed, cancel it and exit the trade
            stop_order = self.client.cancel_order(symbol=self.PAIR1, orderId=stop_order['orderId'])
            exit_order = self.create_market_order(pair=self.PAIR1,
                                                  direction=direction,
                                                  quantity=trade['position_size'])

        print('new exit order: {}'.format(exit_order))
        print('canceled stop order: {}'.format(stop_order))
        trade['exit_order'] = exit_order
        trade['stop_loss_order'] = stop_order # TODO make sure the stop order is canceled
        trade['price_out'] = float(exit_order['price'])  # TODO: make sure the trade is filled before getting the price
        if trade['direction'] == PatternAction.GO_LONG:
            trade['profit'] = (trade['price_out'] - trade['price_in']) * trade['position_size']
        else:
            trade['profit'] = (trade['price_in'] - trade['price_out']) * trade['position_size']
        # calculate exit fees
        self.update_fees(trade, 'exit_order')

        trade['R'] = trade['profit'] / trade['risk']
        trade['time_out'] = datetime.utcnow().isoformat()

        log_template = '{direction} {price_in} {price_out} profit: {profit} risk: {risk} reward: {reward}'
        print(log_template.format(direction=trade['direction'],
                                  price_in=trade['price_in'],
                                  price_out=trade['price_out'],
                                  profit=trade['profit'],
                                  risk=trade['risk'],
                                  reward=trade['R']))


    def run_trend(self):

            self.start_logging()
            self.cancel_all_orders()
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
                    self.cancel_all_orders()
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
                self.cancel_all_orders()
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
                self.cancel_all_orders()
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


