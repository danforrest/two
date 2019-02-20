from binance.client import Client
from binance.websockets import BinanceSocketManager
import os
import sys
import time
from binance import exceptions
import logging
from datetime import datetime, timedelta, date
import requests
import traceback
import gc
import copy
import json
from candlestick_chart import CandleStickChart
from binance.enums import *
from pattern_checker import PatternAction, BullCryptoMovingAverageChecker, BearCryptoMovingAverageChecker
from pattern_checker_test import BullCryptoMovingAverageCheckerTest, BearCryptoMovingAverageCheckerTest, \
    RadgeMeanReversion, PumpReversion
import decimal

HALF_DAY = timedelta(hours=12)
ONE_DAY = timedelta(days=1)

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

    client = None
    bm = None

    # pair priorities USDT always comes last in a pair.  BTC always comes after all
    # coins other than USDT.  ETH comes after all coins other than USDT and BTC.
    # also, pair should go COIN1/COIN3, COIN2/COIN1, COIN2/COIN3
    COIN1 = 'ETH'
    COIN2 = 'USDT'
    PAIR1 = COIN1 + COIN2 #'BTCUSDT'

    QUANTITY_PRECISION = {'BTCUSDT': 6}

    FEE = 0.00075
    MAX_RISK = 0.02

    balance_book = {'timestamp': None,
                    'locked': False,
                    'BTC': {'free': 0.3, 'locked': 0.0},
                    'ETH': {'free': 8.0, 'locked': 0.0},
                    'LTC': {'free': 30.0, 'locked': 0.0},
                    'BNB': {'free': 170.0, 'locked': 0.0},
                    'USDT': {'free': 1000.0, 'locked': 0.0}}

    interval = Client.KLINE_INTERVAL_15MINUTE
    candlesticks = {PAIR1: CandleStickChart(PAIR1, interval)}

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
                        #self.TICK[self.PAIR1] = float(filter['tickSize'])
                        precision = self.num_of_decimal_places(filter['tickSize'])
                        #self.PRICE_PRECISION[self.PAIR1] = precision
                        #self.PRICE_FORMAT[self.PAIR1] = '%.{}f'.format(precision)
                    elif filter['filterType'] == 'LOT_SIZE':
                        #self.MIN_AMOUNT[self.PAIR1] = float(filter['stepSize'])
                        self.QUANTITY_PRECISION[self.PAIR1] = self.num_of_decimal_places(filter['stepSize'])
                    #elif filter['filterType'] == 'MIN_NOTIONAL':
                    #    self.MIN_NOTIONAL[self.PAIR1] = float(filter['minNotional'])


    def update_historical_candlesticks(self):
        current_month = date(year=2017, month=10, day=1)
        end_month = date(year=2019, month=2, day=28)
        while current_month <= end_month:
            print('importing {date:%Y} {date:%m}'.format(date=current_month))
            filename = '{dir}\\{pair}_{interval}_{date:%Y}_{date:%m}_candlesticks.json'.format(dir='data',
                                                                                               pair=self.PAIR1,
                                                                                               interval=self.interval,
                                                                                               date=current_month)
            next_month = date(year=current_month.year + (current_month.month // 12),
                              month=(current_month.month % 12)+1,
                              day=1)
            success = self.candlesticks[self.PAIR1].import_json(filename)
            if not success:
                klines = self.client.get_historical_klines(self.PAIR1,
                                                           self.interval,
                                                           current_month.isoformat(),
                                                           (next_month-timedelta(days=1)).isoformat())
                print('No data for month: {} download instead'.format(current_month.isoformat()))
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
            current_month = next_month

        print('count: {}'.format(len(self.candlesticks[self.PAIR1].chart_data)))

        self.candlesticks[self.PAIR1].recalc_all_metrics()
        print('metrics calculated')

        # the first 100 candles are used to initialize the EMA100 values
        for skip in range(0,100):
            self.candlesticks[self.PAIR1].to_be_processed.get()


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
                trade_list = self.client.get_my_trades(symbol=order['symbol'],
                                                       startTime=start)
                break
            except requests.exceptions.ReadTimeout as e:
                print('Read timeout looking up trades. {}'.format(e))
                print(traceback.format_exc())
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


    def check_trend(self):
        start_time = datetime.utcnow()

        # grab all historical data to initialize metrics
        self.update_historical_candlesticks()

        chart = self.candlesticks[self.PAIR1]
        checker_list = dict()
        checker_list['BullCryptoMA'] = BullCryptoMovingAverageChecker(chart.chart_data)
        checker_list['BearCryptoMA'] = BearCryptoMovingAverageChecker(chart.chart_data)
        # checker_list['PumpReversion'] = PumpReversion(chart.chart_data)
        current_status = PatternAction.WAIT
        current_checker = None
        checker_name = None
        current_trade = None
        stats = {}
        win_count = 0
        win_total = 0
        win_profit = 0
        loss_count = 0
        loss_total = 0
        loss_cost = 0
        fees = 0

        while not self.candlesticks[self.PAIR1].to_be_processed.empty():
            time_stamp = self.candlesticks[self.PAIR1].to_be_processed.get()
            # print('timestamp is ready: {} {}'.format(datetime.utcfromtimestamp(time_stamp / 1000).isoformat(),
            #                                          chart.to_be_processed.qsize()))

            # determine what action we want to take next, if any
            proposed_action = None
            position = None
            if current_status in [PatternAction.WAIT]:
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
                # we are currently in a trade, see if we should exit
                proposed_action, position = current_checker.check_exit(time_stamp)

                # # if we make a substantial gain, update the stop loss point
                # if current_trade is not None and 'price_in' in current_trade:
                #     price_in = current_trade['price_in']
                #     current_price = chart.chart_data[time_stamp].close
                #     if current_trade['direction'] == 'ACTION_SHORT' and price_in / current_price >= 1.04:
                #         percent_jump = current_price / price_in
                #         percent_jump += 0.02
                #         new_stop_loss = price_in * percent_jump # 0.97
                #         print('{} {} Update stop loss point to: {}'.format(price_in, current_price, new_stop_loss))
                #         current_checker.stop_loss = new_stop_loss
                #     elif current_trade['direction'] == 'ACTION_LONG' and current_price / price_in >= 1.04:
                #         percent_jump = current_price / price_in
                #         percent_jump -= 0.02
                #         new_stop_loss = price_in * percent_jump #1.03
                #         print('{} {} Update stop loss point to: {}'.format(price_in, current_price, new_stop_loss))
                #         current_checker.stop_loss = new_stop_loss

            if proposed_action in [PatternAction.GO_LONG, PatternAction.GO_SHORT]:
                # we found an opportunity, enter a trade
                print('{time} {action} {checker}'.format(time=datetime.utcfromtimestamp(time_stamp / 1000).isoformat(),
                                                         action=proposed_action,
                                                         checker=checker_name))
                current_trade = self.enter_trade(direction=proposed_action,
                                                 price_in=chart.chart_data[time_stamp].close,
                                                 stop_loss=current_checker.stop_loss,
                                                 max_risk=self.MAX_RISK * self.balance_book['USDT']['free'])
                current_trade['pattern'] = checker_name
                current_status = PatternAction.HOLD
                current_checker.status = PatternAction.HOLD

            elif proposed_action == PatternAction.EXIT_TRADE:
                # time to exit
                print('{time} {action} {checker}'.format(time=datetime.utcfromtimestamp(time_stamp / 1000).isoformat(),
                                                         action=proposed_action,
                                                         checker=checker_name))
                self.exit_trade(current_trade, price_out=chart.chart_data[time_stamp].close)
                current_status = PatternAction.WAIT
                current_checker.status = PatternAction.WAIT

                # update statistics
                self.update_stats(stats, current_trade, time_stamp)
        # exit the last trade early if we're still in the market
        if current_checker.status == PatternAction.HOLD:
            print('{} {}'.format(datetime.utcfromtimestamp(time_stamp / 1000).isoformat(), 'Forced Exit'))
            self.exit_trade(current_trade, price_out=self.candlesticks[self.PAIR1].chart_data[time_stamp].close)
            self.update_stats(stats, current_trade, time_stamp)

        print('stats: {}'.format(stats))
        stat_template = '{} Wins {:3} = {:8.4f} ({:7.2f}) Losses {:3} = {:8.4f} ({:7.2f}) Fees: {:7.2f} Total: {:7.2f}'
        for month in sorted(stats.keys()):
            stat = stats[month]
            win_count += stat['win_count']
            win_total += stat['win_total']
            win_profit += stat['win_profit']
            loss_count += stat['loss_count']
            loss_total += stat['loss_total']
            loss_cost += stat['loss_cost']
            fees += stat['fees']

            print(stat_template.format(month, stat['win_count'], stat['win_total'], stat['win_profit'],
                                       stat['loss_count'], stat['loss_total'], stat['loss_cost'],
                                       stat['fees'], (stat['win_profit'] + stat['loss_cost'])-stat['fees']))
        print(stat_template.format('Total  ', win_count, win_total, win_profit, loss_count, loss_total, loss_cost,
                                   fees, (win_profit + loss_cost)-fees))
        print('Final balance: {:.4f} {:.4f}'.format(self.balance_book['USDT']['free'], self.balance_book[self.COIN1]['free']))
        end_time = datetime.utcnow()
        print('Runtime: {}'.format(end_time-start_time))
        exit(0)


    def enter_trade(self, direction, price_in, stop_loss, max_risk):
        print('enter balance: {:.4f} {:.4f}'.format(self.balance_book['USDT']['free'], self.balance_book[self.COIN1]['free']))
        #max_risk = self.MAX_RISK * (self.balance_book['USDT']['free'] + self.balance_book[self.COIN1]['free'] * price_in)
        print('max risk: {} {}'.format(max_risk, (self.balance_book['USDT']['free'] + self.balance_book[self.COIN1]['free'] * price_in)))
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

            # update balance
            self.balance_book['USDT']['free'] -= position_size * price_in
            self.balance_book[self.COIN1]['free'] += position_size

        elif direction == PatternAction.GO_SHORT:
            current_balance = self.balance_book[self.COIN1]['free']
            if position_size < 0.9 * current_balance:
                actual_risk = max_risk
            else:
                # balance too small to fund at max risk.
                position_size = 0.9 * current_balance
                actual_risk = position_size * max_retracement

            # update balance
            self.balance_book['USDT']['free'] += position_size * price_in
            self.balance_book[self.COIN1]['free'] -= position_size

        else:
            # Shouldn't happen
            return
        print('trade balance: {:.4f} {:.4f}'.format(self.balance_book['USDT']['free'], self.balance_book[self.COIN1]['free']))

        fee = position_size * price_in * self.FEE
        trade = {'time_in': datetime.utcnow().isoformat(),
                 'direction': direction,
                 'price_in': price_in,
                 'position_size': round(position_size, self.QUANTITY_PRECISION[self.PAIR1]),
                 'risk': actual_risk,
                 'max_risk': max_risk,
                 'fee_in': fee,
                 'max_retracement': max_retracement,
                 'balance_in': {self.COIN1: self.balance_book[self.COIN1]['free'],
                                'USDT': self.balance_book['USDT']['free']}}
        log_template = '{direction} price in: {price_in} max retrace: {retrace} size: {position} risk: {risk}'
        print(log_template.format(direction=direction,
                                  price_in=price_in,
                                  risk=actual_risk,
                                  position=position_size,
                                  retrace=max_retracement))
        return trade


    def exit_trade(self, trade, price_out):
        position_mid = 0.5 * (self.balance_book[self.COIN1]['free'] + (self.balance_book['USDT']['free'] / price_out))
        trade['price_out'] = price_out
        # trade['fee_out'] = trade['position_size'] * trade['price_out'] * self.FEE
        if trade['direction'] == PatternAction.GO_LONG:
            trade['profit'] = (trade['price_out'] - trade['price_in']) * trade['position_size']
            position_out = self.balance_book[self.COIN1]['free'] - position_mid
            self.balance_book['USDT']['free'] += position_out * price_out
            self.balance_book[self.COIN1]['free'] -= position_out
            # self.balance_book['USDT']['free'] += trade['position_size'] * price_out
            # self.balance_book[self.COIN1]['free'] -= trade['position_size']
        else:
            trade['profit'] = (trade['price_in'] - trade['price_out']) * trade['position_size']
            position_out = position_mid - self.balance_book[self.COIN1]['free']
            self.balance_book['USDT']['free'] -= position_out * price_out
            self.balance_book[self.COIN1]['free'] += position_out
            # self.balance_book['USDT']['free'] -= trade['position_size'] * price_out
            # self.balance_book[self.COIN1]['free'] += trade['position_size']
        trade['fee_out'] = position_out * trade['price_out'] * self.FEE

        trade['R'] = trade['profit'] / trade['risk']
        trade['time_out'] = datetime.utcnow().isoformat()

        trade['balance_out'] = {self.COIN1: self.balance_book[self.COIN1]['free'],
                                'USDT': self.balance_book['USDT']['free']}
        log_template = '{direction} {price_in} {price_out} profit: {profit} risk: {risk} reward: {reward}'
        print(log_template.format(direction=trade['direction'],
                                  price_in=trade['price_in'],
                                  price_out=trade['price_out'],
                                  profit=trade['profit'],
                                  risk=trade['risk'],
                                  reward=trade['R']))
        print('exit balance: {:.4f} {:.4f}'.format(self.balance_book['USDT']['free'], self.balance_book[self.COIN1]['free']))


    def update_stats(self, stats, current_trade, time_stamp):
        month = '{date:%Y}-{date:%m}'.format(date=date.fromtimestamp(time_stamp / 1000))
        print('month: {}'.format(month))
        if month not in stats:
            stats[month] = {'win_count': 0,
                            'win_total': 0,
                            'win_profit': 0,
                            'loss_count': 0,
                            'loss_total': 0,
                            'loss_cost': 0,
                            'fees': 0}
        if current_trade['profit'] > 0:
            stats[month]['win_count'] += 1
            stats[month]['win_total'] += current_trade['R']
            stats[month]['win_profit'] += current_trade['profit']
        else:
            stats[month]['loss_count'] += 1
            stats[month]['loss_total'] += current_trade['R']
            stats[month]['loss_cost'] += current_trade['profit']
        stats[month]['fees'] += current_trade['fee_in'] + current_trade['fee_out']


if __name__ == "__main__":
    binance_trend = BinanceTrend()
    binance_trend.check_trend()


