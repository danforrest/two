from datetime import datetime, timedelta
from enum import Enum
from pattern_checker import PatternAction, PatternChecker


class MovingAverageChecker(PatternChecker):
    status = PatternAction.WAIT
    stop_loss = 0
    test_points = 0

    def check_entry(self, index=-1, max_risk=1):
        if self.status == PatternAction.GO_LONG:
            return PatternAction.WAIT, None

        self.stop_loss = 0
        bucket = self.chart.one_minute_chart[index]
        #print('test points: ', self.test_points, 'close: ', bucket.close, 'ema20: ', bucket.metric['ema20'], 'ema50: ', bucket.metric['ema50'], 'ema200: ', bucket.metric['ema200'])
        # 200 EMA is pointing higher
        if 'ema200_delta' in bucket.metric and bucket.metric['ema200_delta'] <= 0:
            #print(datetime.utcfromtimestamp(bucket.time_index).isoformat(), 'delta: ', bucket.metric['ema200_delta'])
            self.test_points = 0
            return PatternAction.WAIT, None

        # price is above 200 EMA
        if bucket.close <= bucket.metric['ema200']:
            #print(datetime.utcfromtimestamp(bucket.time_index).isoformat(), 'ema200: ', bucket.close, bucket.metric['ema200'])
            return PatternAction.WAIT, None

        # price is above 50 EMA
        if bucket.close <= bucket.metric['ema50']:
            #print(datetime.utcfromtimestamp(bucket.time_index).isoformat(), 'ema50: ', bucket.close, bucket.metric['ema50'])
            return PatternAction.WAIT, None

        # check for 2 tests of the dynamic support (20 EMA and 50 EMA)
        if bucket.close <= bucket.metric['ema20']:
            self.test_points += 1
            #print(datetime.utcfromtimestamp(bucket.time_index).isoformat(), 'ema20: ', bucket.close, bucket.metric['ema20'])
            return PatternAction.WAIT, None

        if self.test_points < 2:
            #print(datetime.utcfromtimestamp(bucket.time_index).isoformat(), 'not enough test points: ', self.test_points)
            return PatternAction.WAIT, None

        # if bucket.metric['atr'] < 1.0:
        #     # not enough volatility.  stop loss would be too tight.
        #     return PatternAction.WAIT, None

        # go long
        self.status = PatternAction.GO_LONG
        self.stop_loss = bucket.close - (2 * bucket.metric['atr'])
        size = (max_risk / (2 * bucket.metric['atr']))
        return PatternAction.GO_LONG, {'price': bucket.close,
                                   'limit': None,
                                   'stop': self.stop_loss,
                                   'size': size
                                       }

    def check_exit(self, index=-1):
        if self.status != PatternAction.GO_LONG:
            return PatternAction.WAIT, None

        bucket = self.chart.one_minute_chart[index]

        if bucket.low < self.stop_loss:
            print('Selling due to stop loss')
            self.status = PatternAction.GO_SHORT
            return PatternAction.GO_SHORT, self.stop_loss

        if bucket.close >= bucket.metric['ema50']:
            return PatternAction.WAIT, None

        self.status = PatternAction.GO_SHORT
        return PatternAction.GO_SHORT, None


class SwingPriceChecker(PatternChecker):
    status = PatternAction.WAIT
    stop_loss = 0
    target_price = 0

    def check_entry(self, index=-1, max_risk=1):
        if self.status == PatternAction.GO_LONG:
            return PatternAction.WAIT, None

        self.stop_loss = 0
        bucket = self.chart.one_minute_chart[index]
        # 50 EMA is pointing higher
        if 'ema50_delta' in bucket.metric and bucket.metric['ema50_delta'] <= 0:
            # print('delta: ', bucket.metric['ema200_delta'])
            return PatternAction.WAIT, None

        # price is above 50 EMA
        if bucket.close <= bucket.metric['ema50']:
            # print('ema200: ', bucket.close, bucket.metric['ema200'])
            return PatternAction.WAIT, None

        # price is above 20 EMA
        if bucket.close <= bucket.metric['ema20']:
            # print('ema50: ', bucket.close, bucket.metric['ema50'])
            return PatternAction.WAIT, None

        # check for 2 tests of the dynamic support (20 EMA and 50 EMA)
        if False:
            return PatternAction.WAIT, None

        # if bucket.metric['atr'] < 1.0:
        #     # not enough volatility.  stop loss would be too tight.
        #     return PatternAction.WAIT

        # go long
        self.status = PatternAction.GO_LONG
        self.stop_loss = bucket.close - (2 * bucket.metric['atr'])
        self.target_price = bucket.close + (1 * bucket.metric['atr'])
        size = (max_risk / (2 * bucket.metric['atr']))
        return PatternAction.GO_LONG, {'price': bucket.close,
                                   'limit': None,
                                   'stop': self.stop_loss,
                                   'size': size
                                       }

    def check_exit(self, index=-1):
        if self.status != PatternAction.GO_LONG:
            return PatternAction.WAIT, None

        bucket = self.chart.one_minute_chart[index]

        if bucket.low < self.stop_loss:
            print('Selling due to stop loss')
            self.status = PatternAction.GO_SHORT
            return PatternAction.GO_SHORT, self.stop_loss

        if bucket.close >= self.target_price:
            self.stop_loss = bucket.close - (2 * bucket.metric['atr'])
            self.target_price = bucket.close + (1 * bucket.metric['atr'])
            #self.status = PatternAction.SELL
            return PatternAction.WAIT, None

        return PatternAction.WAIT, None


class DonchianChannelBreakoutChecker(PatternChecker):
    status = PatternAction.WAIT
    entry_index = 0
    stop_loss = 0

    def check_entry(self, index=-1, max_risk=1):
        if self.status == PatternAction.GO_LONG:
            return PatternAction.WAIT, None

        bucket = self.chart.one_minute_chart[index]

        # check if crossed 20 period high mark
        if 'range20_max' in bucket.metric and bucket.high < bucket.metric['range20_max']:
            return PatternAction.WAIT, None

        # go long
        self.status = PatternAction.GO_LONG
        self.entry_index = index
        print('index: ', index, 'atr: ', bucket.metric['atr'])
        #self.stop_loss = bucket.close - (2 * bucket.metric['atr'])
        self.stop_loss = bucket.metric['range20_min']
        size = (max_risk / (bucket.metric['range20_max'] - bucket.metric['range20_min']))
        return PatternAction.GO_LONG, {'price': bucket.close,
                                   'size': size,
                                   'stop': self.stop_loss
                                       }

    def check_exit(self, index=-1):
        if self.status != PatternAction.GO_LONG:
            return PatternAction.WAIT, None
        if index == self.entry_index:
            # don't by and sell on the same bucket
            return PatternAction.WAIT, None

        bucket = self.chart.one_minute_chart[index]

        if bucket.low < self.stop_loss:
            print('Selling due to stop loss')
            self.status = PatternAction.GO_SHORT
            return PatternAction.GO_SHORT, self.stop_loss

        # check if crossed 20 period high mark
        if 'range20_min' in bucket.metric and bucket.low > bucket.metric['range20_min']:
            self.stop_loss = bucket.metric['range20_min']
            return PatternAction.WAIT, None

        self.status = PatternAction.GO_SHORT
        return PatternAction.GO_SHORT, None


class DonchianMacdChecker(PatternChecker):
    status = PatternAction.WAIT
    entry_index = 0
    stop_loss = 0
    range20_entry = False
    macd_entry = False

    def check_entry(self, index=-1, max_risk=1):
        if self.status == PatternAction.GO_LONG:
            return PatternAction.WAIT, None

        bucket = self.chart.one_minute_chart[index]

        # check if crossed 20 period high mark
        if 'range20_max' in bucket.metric and bucket.high >= bucket.metric['range20_max']:
            self.range20_entry = True
        else:
            self.range20_entry = False

        # check if ema12 crossed above ema26
        if 'macd_histogram' in bucket.metric:
            if bucket.metric['macd_histogram'] < 0:
                self.macd_entry = True
            elif bucket.metric['macd_histogram'] > 0:
                self.macd_entry = False

        if self.range20_entry is False or self.macd_entry is False:
            return PatternAction.WAIT, None

        # go long
        self.status = PatternAction.GO_LONG
        self.entry_index = index
        print('index: ', index, 'atr: ', bucket.metric['atr'])
        self.stop_loss = bucket.close - (2 * bucket.metric['atr'])
        size = (max_risk / (2 * bucket.metric['atr']))
        return PatternAction.GO_LONG, {'price': bucket.close,
                                   'size': size,
                                   'stop': self.stop_loss
                                       }

    def check_exit(self, index=-1):
        if self.status != PatternAction.GO_LONG:
            return PatternAction.WAIT, None
        if index == self.entry_index:
            # don't by and sell on the same bucket
            return PatternAction.WAIT, None

        bucket = self.chart.one_minute_chart[index]

        if bucket.low < self.stop_loss:
            print('Selling due to stop loss')
            self.status = PatternAction.GO_SHORT
            return PatternAction.GO_SHORT, self.stop_loss

        # check if crossed 20 period low mark
        if 'range20_min' in bucket.metric and bucket.low > bucket.metric['range20_min']:
            #self.stop_loss = max(bucket.metric['range20_min'], self.stop_loss)
            return PatternAction.WAIT, None

        # check if ema12 crossed below ema26
        if 'macd_histogram' in bucket.metric and bucket.metric['macd_histogram'] < 0:
            return PatternAction.WAIT, None

        self.status = PatternAction.GO_SHORT
        self.range20_entry = False
        self.macd_entry = False
        return PatternAction.GO_SHORT, None


class DonchianChannelDanChecker(PatternChecker):
    status = PatternAction.WAIT
    entry_index = 0
    stop_loss = 0
    top_tapped = False

    def check_entry(self, index=-1, max_risk=1):
        if self.status == PatternAction.GO_LONG:
            return PatternAction.WAIT, None

        bucket = self.chart.one_minute_chart[index]

        if 'range20_max' not in bucket.metric or 'range20_min' not in bucket.metric or 'ema50' not in bucket.metric:
            return PatternAction.WAIT, None

        if bucket.high >= bucket.metric['range20_max']:
            self.top_tapped = True

        if bucket.metric['ema50_delta'] <= 0:
            self.top_tapped = False
            return PatternAction.WAIT, None

        if bucket.close <= bucket.metric['ema50']:
            return PatternAction.WAIT

        # check if above the donchian mid point
        if bucket.close < 0.5 * (bucket.metric['range20_max'] + bucket.metric['range20_min']):
            return PatternAction.WAIT, None

        if self.top_tapped == False:
            return PatternAction.WAIT, None

        # go long
        self.status = PatternAction.GO_LONG
        self.entry_index = index
        risk = 2 * bucket.metric['atr']
        self.stop_loss = bucket.close - risk
        #self.stop_loss = bucket.metric['range20_min']
        size = max_risk / risk
        return PatternAction.GO_LONG, {'price': bucket.close,
                                   'size': size,
                                   'stop': self.stop_loss
                                       }

    def check_exit(self, index=-1):
        if self.status != PatternAction.GO_LONG:
            return PatternAction.WAIT, None
        if index == self.entry_index:
            # don't by and sell on the same bucket
            return PatternAction.WAIT, None

        bucket = self.chart.one_minute_chart[index]

        if bucket.low < self.stop_loss:
            print('Selling due to stop loss')
            self.status = PatternAction.GO_SHORT
            return PatternAction.GO_SHORT, self.stop_loss

        # check if crossed 20 period high mark
        # add donchian10 channel to adjust the exit criteria

        if bucket.low > bucket.metric['range20_min']:
            self.stop_loss = max(self.stop_loss, bucket.metric['range20_min'])
            return PatternAction.WAIT, None

        self.status = PatternAction.GO_SHORT
        return PatternAction.GO_SHORT, None


class MagicDanChecker(PatternChecker):
    status = PatternAction.WAIT
    entry_index = 0
    stop_loss = 0
    top_tapped = False
    ready_to_buy = False
    ready_to_sell = False
    days_to_buy = 20
    days_to_sell = 20
    zero_price = 0.0

    def check_entry(self, index=-1, max_risk=1):
        if self.status == PatternAction.GO_LONG:
            return PatternAction.WAIT, None

        bucket = self.chart.one_minute_chart[index]

        # if bucket.metric['rsi14'] > 70 or bucket.metric['stochastic14_oscillator'] > 80:
        #     self.ready_to_buy = False

        if bucket.metric['rsi14'] < 30 and bucket.metric['stochastic14_oscillator'] < 20:
            # print('both charts are over sold')
            self.ready_to_buy = True
            self.days_to_buy = 30

        if self.days_to_buy <= 0:
            self.ready_to_buy = False
            return PatternAction.WAIT, None
        self.days_to_buy -= 1

        if bucket.metric['ema20_delta'] < 0.00:
            # print('ema hasnt flattened out yet', bucket.metric['ema20_delta'])
            return PatternAction.WAIT, None

        if self.ready_to_buy == False:
            # print('not ready to buy')
            return PatternAction.WAIT, None

        # go long
        self.status = PatternAction.GO_LONG
        self.entry_index = index
        risk = 2 * bucket.metric['atr']
        #risk = bucket.close - bucket.metric['range14_min']
        self.stop_loss = bucket.close - risk
        #self.stop_loss = bucket.metric['range20_min']
        size = max_risk / risk
        self.ready_to_buy = False
        self.ready_to_sell = False
        self.zero_price = bucket.close
        return PatternAction.GO_LONG, {'price': bucket.close,
                                   'size': size,
                                   'stop': self.stop_loss
                                       }

    def check_exit(self, index=-1):
        if self.status != PatternAction.GO_LONG:
            return PatternAction.WAIT, None
        if index == self.entry_index:
            # don't by and sell on the same bucket
            return PatternAction.WAIT, None

        bucket = self.chart.one_minute_chart[index]

        if bucket.low < self.stop_loss:
            print('Selling due to stop loss')
            self.status = PatternAction.GO_SHORT
            self.ready_to_buy = False
            self.ready_to_sell = False
            return PatternAction.GO_SHORT, self.stop_loss

        # if bucket.close > self.zero_price + (1 * bucket.metric['atr']):
        #     self.stop_loss += bucket.metric['atr']

        # if bucket.metric['rsi14'] < 30 or bucket.metric['stochastic14_oscillator'] < 20:
        #     self.ready_to_sell = False

        if bucket.metric['rsi14'] > 70 and bucket.metric['stochastic14_oscillator'] > 80:
            self.ready_to_sell = True
            self.days_to_sell = 30

        if self.days_to_sell <= 0:
            self.ready_to_sell = False
            return PatternAction.WAIT, None
        self.days_to_sell -= 1

        if bucket.metric['ema20_delta'] > -0.00:
            return PatternAction.WAIT, None

        self.status = PatternAction.GO_SHORT
        self.ready_to_buy = False
        self.ready_to_sell = False
        return PatternAction.GO_SHORT, None


class RadgeMeanReversion(PatternChecker):
    status = PatternAction.WAIT
    stop_loss = 0
    soft_stop = 0
    lower_low_count = 0
    previous_low = 0
    previous_close = 0

    def check_entry(self, index):
        if self.status not in [PatternAction.WAIT]:
            return self.status, None

        self.stop_loss = 0
        bucket = self.chart[index]
        time_string = datetime.utcfromtimestamp(index/1000).isoformat()

        # need 3 lower lows
        current_low = bucket.low
        if current_low < self.previous_low:
            self.lower_low_count += 1
        else:
            self.lower_low_count = 0
        self.previous_low = current_low
        if self.lower_low_count < 3:
            print(time_string, 'consecutive lows: ', self.lower_low_count)
            return PatternAction.WAIT, None

        # price closes above 100 EMA
        if 'ema100' in bucket.metric and (bucket.close <= bucket.metric['ema100']):
            print(time_string, 'ema100: ', bucket.metric['ema100'])
            return PatternAction.WAIT, None

        # price closes below 5 EMA
        if 'ema5' in bucket.metric and (bucket.close >= bucket.metric['ema5']):
            print(time_string, 'ema5: ', bucket.metric['ema5'])
            return PatternAction.WAIT, None

        # go long
        print('lows {}'.format(self.lower_low_count))
        print('ema100 {}'.format(bucket.metric['ema100']))
        print('ema5 {}'.format(bucket.metric['ema5']))
        print('open {}'.format(bucket.open))
        print('close {}'.format(bucket.close))
        self.status = PatternAction.GO_LONG
        self.stop_loss = bucket.metric['ema100']
        self.previous_close = bucket.close
        if bucket.close - bucket.metric['atr'] > bucket.metric['ema100']:
            self.stop_loss = bucket.close - bucket.metric['atr']
        return PatternAction.GO_LONG, {'price': bucket.close,
                                       'stop': self.stop_loss,
                                      }

    def check_exit(self, index):

        #### TODO: Exit immediately or drop stop loss to a break even/slight win if lower time frame
        #### TODO  jumps fast against you

        if self.status not in [PatternAction.HOLD]:
            return self.status, None

        bucket = self.chart[index]

        self.soft_stop = min(self.stop_loss, bucket.metric['ema100'])

        if bucket.close > self.previous_close:
            # print('Selling due to stop loss')
            self.status = PatternAction.EXIT_TRADE
            return PatternAction.EXIT_TRADE, bucket.close

        self.previous_close = bucket.close
        return PatternAction.HOLD, None


class BullCryptoMovingAverageCheckerTest(PatternChecker):
    status = PatternAction.WAIT
    stop_loss = 0
    soft_stop = 0

    def check_entry(self, index):
        if self.status not in [PatternAction.WAIT]:
            return self.status, None

        self.stop_loss = 0
        bucket = self.chart[index]
        time_string = datetime.utcfromtimestamp(index/1000).isoformat()

        # 100 EMA is pointing higher
        if 'ema100_delta' in bucket.metric and bucket.metric['ema100_delta'] <= 0:
            print(time_string, 'ema100_delta: ', bucket.metric['ema100_delta'])
            return PatternAction.WAIT, None

        # 50 EMA is pointing higher
        if 'ema50_delta' in bucket.metric and bucket.metric['ema50_delta'] <= 0:
            print(time_string, 'ema50_delta: ', bucket.metric['ema50_delta'])
            return PatternAction.WAIT, None

        # # 25 EMA is pointing higher
        # if 'ema25_delta' in bucket.metric and bucket.metric['ema25_delta'] <= 0:
        #     print(time_string, 'ema25_delta: ', bucket.metric['ema25_delta'])
        #     return PatternAction.WAIT, None

        # 15 EMA is pointing higher
        if 'ema15_delta' in bucket.metric and bucket.metric['ema15_delta'] <= 0:
            print(time_string, 'ema15_delta: ', bucket.metric['ema15_delta'])
            return PatternAction.WAIT, None

        # 5 EMA is pointing higher
        if 'ema5_delta' in bucket.metric and bucket.metric['ema5_delta'] <= 0:
            print(time_string, 'ema5_delta: ', bucket.metric['ema5_delta'])
            return PatternAction.WAIT, None

        # 50 EMA is above 100 EMA
        if bucket.metric['ema50'] <= bucket.metric['ema100']:
            print(time_string, 'ema50: ', bucket.metric['ema50'], bucket.metric['ema100'])
            return PatternAction.WAIT, None

        # # 25 EMA is above 50 EMA
        # if bucket.metric['ema25'] <= bucket.metric['ema50']:
        #     print(time_string, 'ema25: ', bucket.metric['ema25'], bucket.metric['ema50'])
        #     return PatternAction.WAIT, None

        # 15 EMA is above 50 EMA
        if bucket.metric['ema15'] <= bucket.metric['ema50']:
            print(time_string, 'ema15: ', bucket.metric['ema15'], bucket.metric['ema50'])
            return PatternAction.WAIT, None

        # # 5 EMA is above 25 EMA
        # if bucket.metric['ema5'] <= bucket.metric['ema25']:
        #     print(time_string, 'ema5: ', bucket.metric['ema5'], bucket.metric['ema25'])
        #     return PatternAction.WAIT, None

        # 5 EMA is above 15 EMA
        if bucket.metric['ema5'] <= bucket.metric['ema15']:
            print(time_string, 'ema5: ', bucket.metric['ema5'], bucket.metric['ema15'])
            return PatternAction.WAIT, None

        # # price open is above 25 EMA
        # if bucket.open <= bucket.metric['ema25']:
        #     print(time_string, 'open: ', bucket.open, bucket.metric['ema25'])
        #     return PatternAction.WAIT, None

        # price open is above 15 EMA
        if bucket.open <= bucket.metric['ema15']:
            print(time_string, 'open: ', bucket.open, bucket.metric['ema15'])
            return PatternAction.WAIT, None

        # price close is above 5 EMA
        if bucket.close <= bucket.metric['ema5']:
            print(time_string, 'close: ', bucket.close, bucket.metric['ema5'])
            return PatternAction.WAIT, None

        # price was not rejected
        # if bucket.close <= bucket.metric['ema50']:
        #     #print(time_string, 'ema50: ', bucket.close, bucket.metric['ema50'])
        #     return PatternAction.WAIT, None

        # if bucket.metric['atr'] > 0.01 * bucket.close:
        #     # too much volatility.
        #     print(time_string, 'atr: ', bucket.metric['atr'], 0.01 * bucket.close)
        #     return PatternAction.WAIT, None

        # go long
        print('ema100 delta {}'.format(bucket.metric['ema100_delta']))
        print('ema50 delta {}'.format(bucket.metric['ema50_delta']))
        print('ema25 delta {}'.format(bucket.metric['ema25_delta']))
        print('ema15 delta {}'.format(bucket.metric['ema15_delta']))
        print('ema5 delta {}'.format(bucket.metric['ema5_delta']))
        print('ema100 {}'.format(bucket.metric['ema100']))
        print('ema50 {}'.format(bucket.metric['ema50']))
        print('ema25 {}'.format(bucket.metric['ema25']))
        print('ema15 {}'.format(bucket.metric['ema15']))
        print('ema5 {}'.format(bucket.metric['ema5']))
        print('open {}'.format(bucket.open))
        print('close {}'.format(bucket.close))
        print('atr {}'.format(bucket.metric['atr']))
        self.status = PatternAction.GO_LONG
        self.stop_loss = min(bucket.metric['ema100'], bucket.metric['ema200'])
        if bucket.close - 6*bucket.metric['atr'] < self.stop_loss:
            self.stop_loss = bucket.close - 6*bucket.metric['atr']
        return PatternAction.GO_LONG, {'price': bucket.close,
                                       'stop': self.stop_loss,
                                       }

    def check_exit(self, index):

        #### TODO: Exit immediately or drop stop loss to a break even/slight win if lower time
        #### TODO  jumps fast against you

        if self.status not in [PatternAction.HOLD]:
            return self.status, None

        bucket = self.chart[index]

        self.soft_stop = max(self.stop_loss, min(bucket.metric['ema100'], bucket.metric['ema200']))
        if bucket.low < self.stop_loss:
            # print('Selling due to stop loss')
            self.status = PatternAction.EXIT_TRADE
            return PatternAction.EXIT_TRADE, self.stop_loss
        if bucket.close < self.soft_stop:
            # print('Selling due to stop loss')
            self.status = PatternAction.EXIT_TRADE
            return PatternAction.EXIT_TRADE, self.soft_stop

        return PatternAction.HOLD, None


class BearCryptoMovingAverageCheckerTest(PatternChecker):
    status = PatternAction.WAIT
    stop_loss = 0
    soft_stop = 0

    def check_entry(self, index):
        if self.status not in [PatternAction.WAIT]:
            return self.status, None

        self.stop_loss = 0
        bucket = self.chart[index]
        time_string = datetime.utcfromtimestamp(index/1000).isoformat()

        # 100 EMA is pointing lower
        if 'ema100_delta' in bucket.metric and bucket.metric['ema100_delta'] >= 0:
            print(time_string, 'ema100_delta: ', bucket.metric['ema100_delta'])
            return PatternAction.WAIT, None

        # 50 EMA is pointing lower
        if 'ema50_delta' in bucket.metric and bucket.metric['ema50_delta'] >= 0:
            print(time_string, 'ema50_delta: ', bucket.metric['ema50_delta'])
            return PatternAction.WAIT, None

        # # 25 EMA is pointing lower
        # if 'ema25_delta' in bucket.metric and bucket.metric['ema25_delta'] >= 0:
        #     print(time_string, 'ema25_delta: ', bucket.metric['ema25_delta'])
        #     return PatternAction.WAIT, None

        # 15 EMA is pointing lower
        if 'ema15_delta' in bucket.metric and bucket.metric['ema15_delta'] >= 0:
            print(time_string, 'ema15_delta: ', bucket.metric['ema15_delta'])
            return PatternAction.WAIT, None

        # 5 EMA is pointing lower
        if 'ema5_delta' in bucket.metric and bucket.metric['ema5_delta'] >= 0:
            print(time_string, 'ema5_delta: ', bucket.metric['ema5_delta'])
            return PatternAction.WAIT, None

        # 50 EMA is below 100 EMA
        if bucket.metric['ema50'] >= bucket.metric['ema100']:
            print(time_string, 'ema50: ', bucket.metric['ema50'], bucket.metric['ema100'])
            return PatternAction.WAIT, None

        # 25 EMA is below 50 EMA
        if bucket.metric['ema25'] >= bucket.metric['ema50']:
            print(time_string, 'ema25: ', bucket.metric['ema25'], bucket.metric['ema50'])
            return PatternAction.WAIT, None

        # 15 EMA is below 50 EMA
        if bucket.metric['ema15'] >= bucket.metric['ema50']:
            print(time_string, 'ema15: ', bucket.metric['ema15'], bucket.metric['ema50'])
            return PatternAction.WAIT, None

        # # 5 EMA is below 25 EMA
        # if bucket.metric['ema5'] >= bucket.metric['ema25']:
        #     print(time_string, 'ema5: ', bucket.metric['ema5'], bucket.metric['ema25'])
        #     return PatternAction.WAIT, None

        # 5 EMA is below 15 EMA
        if bucket.metric['ema5'] >= bucket.metric['ema15']:
            print(time_string, 'ema5: ', bucket.metric['ema5'], bucket.metric['ema15'])
            return PatternAction.WAIT, None

        # # price open is below 25 EMA
        # if bucket.open >= bucket.metric['ema25']:
        #     print(time_string, 'open: ', bucket.open, bucket.metric['ema25'])
        #     return PatternAction.WAIT, None

        # price open is below 15 EMA
        if bucket.open >= bucket.metric['ema15']:
            print(time_string, 'open: ', bucket.open, bucket.metric['ema15'])
            return PatternAction.WAIT, None

        # price close is below 5 EMA
        if bucket.close >= bucket.metric['ema5']:
            print(time_string, 'close: ', bucket.close, bucket.metric['ema5'])
            return PatternAction.WAIT, None

        # price was not rejected
        # if bucket.close <= bucket.metric['ema50']:
        #     #print(time_string, 'ema50: ', bucket.close, bucket.metric['ema50'])
        #     return PatternAction.WAIT, None

        # if bucket.metric['atr'] > 0.01 * bucket.close:
        #     # too much volatility.
        #     print(time_string, 'too volatile atr: ', bucket.metric['atr'], 0.01 * bucket.close)
        #     return PatternAction.WAIT, None

        # go short
        print('ema100 delta {}'.format(bucket.metric['ema100_delta']))
        print('ema50 delta {}'.format(bucket.metric['ema50_delta']))
        print('ema25 delta {}'.format(bucket.metric['ema25_delta']))
        print('ema15 delta {}'.format(bucket.metric['ema15_delta']))
        print('ema5 delta {}'.format(bucket.metric['ema5_delta']))
        print('ema200 {}'.format(bucket.metric['ema200']))
        print('ema100 {}'.format(bucket.metric['ema100']))
        print('ema50 {}'.format(bucket.metric['ema50']))
        print('ema25 {}'.format(bucket.metric['ema25']))
        print('ema15 {}'.format(bucket.metric['ema15']))
        print('ema5 {}'.format(bucket.metric['ema5']))
        print('open {}'.format(bucket.open))
        print('close {}'.format(bucket.close))
        print('atr {}'.format(bucket.metric['atr']))
        self.status = PatternAction.GO_SHORT
        self.stop_loss = max(bucket.metric['ema100'], bucket.metric['ema200'])
        if bucket.close + 6*bucket.metric['atr'] > self.stop_loss:
            self.stop_loss = bucket.close + 6*bucket.metric['atr']
        return PatternAction.GO_SHORT, {'price': bucket.close,
                                        'stop': self.stop_loss,
                                       }

    def check_exit(self, index):

        #### TODO: Exit immediately or drop stop loss to a break even/slight win if lower time frame
        #### TODO  jumps fast against you

        if self.status not in [PatternAction.HOLD]:
            return self.status, None

        bucket = self.chart[index]

        self.soft_stop = min(self.stop_loss, max(bucket.metric['ema100'], bucket.metric['ema200']))

        if bucket.high > self.stop_loss:
            # print('Selling due to stop loss')
            self.status = PatternAction.EXIT_TRADE
            return PatternAction.EXIT_TRADE, self.stop_loss
        if bucket.close > self.soft_stop:
            # print('Selling due to stop loss')
            self.status = PatternAction.EXIT_TRADE
            return PatternAction.EXIT_TRADE, self.soft_stop

        return PatternAction.HOLD, None


class PumpReversion(PatternChecker):
    status = PatternAction.WAIT
    stop_loss = 0
    low_point = None
    high_point = None
    retest_wait = None
    entry_price = None
    exit_price = None
    exit_time = None
    THRESHOLD = 1.01
    MAX_TIME = timedelta(days=2.0)
    RETEST_TIME = timedelta(minutes=20.0)
    current_stage = None

    PRE_PUMP = 'PRE_PUMP'
    PUMP = 'PUMP'
    POST_PUMP = 'POST_PUMP'
    RETEST_HIGH = 'RETEST_HIGH'
    UNWIND = 'UNWIND'

    def check_entry(self, index):
        if self.status not in [PatternAction.WAIT]:
            return self.status, None

        self.stop_loss = 0
        bucket = self.chart[index]
        time_string = datetime.utcfromtimestamp(index/1000).isoformat()

        if self.current_stage == self.PRE_PUMP:
            if bucket.close < bucket.open:
                self.low_point = bucket.close
                return PatternAction.WAIT, None
            else:
                self.current_stage = self.PUMP

        if self.current_stage == self.PUMP:
            if bucket.open <= bucket.close:
                self.high_point = bucket.close
                return PatternAction.WAIT, None
            else:
                if self.high_point / self.low_point < self.THRESHOLD:
                    # didn't pump enough, reset
                    self.low_point = bucket.close
                    self.high_point = None
                    self.retest_wait = None
                    self.current_stage = self.PRE_PUMP
                    return PatternAction.WAIT, None
                else:
                    self.current_stage = self.POST_PUMP
                    self.retest_wait = datetime.utcnow() + self.RETEST_TIME
                    self.entry_price = self.high_point
                    self.exit_price = (self.high_point + self.low_point) / 2.0
                    self.low_point = bucket.close
                    self.high_point = None

        if self.current_stage == self.POST_PUMP:
            if bucket.high > self.entry_price:
                # going short!
                self.current_stage = self.UNWIND
                self.stop_loss = self.exit_price
                self.status = PatternAction.GO_SHORT
                self.exit_time = datetime.utcnow() + self.MAX_TIME

                return PatternAction.GO_SHORT, {'price': bucket.close,
                                                'stop': self.stop_loss
                                                }
            elif datetime.utcnow() > self.retest_wait:
                # took too long, going back to pre pump stage
                if bucket.close < bucket.open:
                    self.low_point = bucket.close
                self.current_stage = self.PRE_PUMP
                self.retest_wait = None
                self.entry_price = None
                self.exit_price = None
                return PatternAction.WAIT, None

        if self.current_stage == self.UNWIND:
            if bucket.close < bucket.open:
                self.low_point = bucket.close
            else:
                self.low_point = None

        return PatternAction.WAIT, None

        # find the low point before the pump
        if bucket.close < bucket.open and not self.high_point:
            self.low_point = bucket.close
            return PatternAction.WAIT, None

        # find the top of the pump
        if bucket.open <= bucket.close and self.low_point:
            if not self.retest_wait:
                # this is the first time hitting the top of the pump, just mark it and wait
                # till we re-test the high point.
                self.high_point = bucket.close
                return PatternAction.WAIT, None
            elif datetime.utcnow() < self.retest_wait:
                # we just hit the top of the pump a second time.  go short
                self.status = PatternAction.GO_SHORT
                self.stop_loss = (self.high_point + self.low_point) / 2
                self.exit_time = datetime.utcnow() + self.MAX_TIME

                return PatternAction.GO_SHORT, {'price': bucket.close,
                                                'stop': self.stop_loss
                                                }
            else:
                self.low_point = None
                self.high_point = None
                self.retest_wait = None

        # we just peaked (1+ candle retracement).
        if self.high_point / self.low_point <= self.THRESHOLD:
            self.low_point = None
            self.high_point = None
            self.retest_wait = None
            return PatternAction.WAIT, None

        return PatternAction.WAIT, None




    def check_exit(self, index):

        #### TODO: Exit immediately or drop stop loss to a break even/slight win if lower time frame
        #### TODO  jumps fast against you

        if self.status not in [PatternAction.HOLD]:
            return self.status, None

        bucket = self.chart[index]

        if bucket.close > self.stop_loss:
            print('Selling price target hit')
            self.status = PatternAction.EXIT_TRADE
            return PatternAction.EXIT_TRADE, bucket.close

        return PatternAction.HOLD, None


