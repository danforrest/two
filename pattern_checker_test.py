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

        # 25 EMA is pointing higher
        if 'ema25_delta' in bucket.metric and bucket.metric['ema25_delta'] <= 0:
            print(time_string, 'ema25_delta: ', bucket.metric['ema25_delta'])
            return PatternAction.WAIT, None

        # # 15 EMA is pointing higher
        # if 'ema15_delta' in bucket.metric and bucket.metric['ema15_delta'] <= 0:
        #     print(time_string, 'ema15_delta: ', bucket.metric['ema15_delta'])
        #     return PatternAction.WAIT, None

        # 5 EMA is pointing higher
        if 'ema5_delta' in bucket.metric and bucket.metric['ema5_delta'] <= 0:
            print(time_string, 'ema5_delta: ', bucket.metric['ema5_delta'])
            return PatternAction.WAIT, None

        # 50 EMA is above 100 EMA
        if bucket.metric['ema50'] <= bucket.metric['ema100']:
            print(time_string, 'ema50: ', bucket.metric['ema50'], bucket.metric['ema100'])
            return PatternAction.WAIT, None

        # 25 EMA is above 50 EMA
        if bucket.metric['ema25'] <= bucket.metric['ema50']:
            print(time_string, 'ema25: ', bucket.metric['ema25'], bucket.metric['ema50'])
            return PatternAction.WAIT, None

        # # 15 EMA is above 50 EMA
        # if bucket.metric['ema15'] <= bucket.metric['ema50']:
        #     print(time_string, 'ema15: ', bucket.metric['ema15'], bucket.metric['ema50'])
        #     return PatternAction.WAIT, None

        # 5 EMA is above 25 EMA
        if bucket.metric['ema5'] <= bucket.metric['ema25']:
            print(time_string, 'ema5: ', bucket.metric['ema5'], bucket.metric['ema25'])
            return PatternAction.WAIT, None

        # # 5 EMA is above 15 EMA
        # if bucket.metric['ema5'] <= bucket.metric['ema15']:
        #     print(time_string, 'ema5: ', bucket.metric['ema5'], bucket.metric['ema15'])
        #     return PatternAction.WAIT, None

        # price open is above 25 EMA
        if bucket.open <= bucket.metric['ema25']:
            print(time_string, 'open: ', bucket.open, bucket.metric['ema25'])
            return PatternAction.WAIT, None

        # # price open is above 15 EMA
        # if bucket.open <= bucket.metric['ema15']:
        #     print(time_string, 'open: ', bucket.open, bucket.metric['ema15'])
        #     return PatternAction.WAIT, None

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
        if bucket.close - 4*bucket.metric['atr'] < self.stop_loss:
            self.stop_loss = bucket.close - 4*bucket.metric['atr']
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
            return PatternAction.EXIT_TRADE, bucket.close

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

        # 25 EMA is pointing lower
        if 'ema25_delta' in bucket.metric and bucket.metric['ema25_delta'] >= 0:
            print(time_string, 'ema25_delta: ', bucket.metric['ema25_delta'])
            return PatternAction.WAIT, None

        # # 15 EMA is pointing lower
        # if 'ema15_delta' in bucket.metric and bucket.metric['ema15_delta'] >= 0:
        #     print(time_string, 'ema15_delta: ', bucket.metric['ema15_delta'])
        #     return PatternAction.WAIT, None

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

        # # 15 EMA is below 50 EMA
        # if bucket.metric['ema15'] >= bucket.metric['ema50']:
        #     print(time_string, 'ema15: ', bucket.metric['ema15'], bucket.metric['ema50'])
        #     return PatternAction.WAIT, None

        # 5 EMA is below 25 EMA
        if bucket.metric['ema5'] >= bucket.metric['ema25']:
            print(time_string, 'ema5: ', bucket.metric['ema5'], bucket.metric['ema25'])
            return PatternAction.WAIT, None

        # # 5 EMA is below 15 EMA
        # if bucket.metric['ema5'] >= bucket.metric['ema15']:
        #     print(time_string, 'ema5: ', bucket.metric['ema5'], bucket.metric['ema15'])
        #     return PatternAction.WAIT, None

        # price open is below 25 EMA
        if bucket.open >= bucket.metric['ema25']:
            print(time_string, 'open: ', bucket.open, bucket.metric['ema25'])
            return PatternAction.WAIT, None

        # # price open is below 15 EMA
        # if bucket.open >= bucket.metric['ema15']:
        #     print(time_string, 'open: ', bucket.open, bucket.metric['ema15'])
        #     return PatternAction.WAIT, None

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
        if bucket.close + 4*bucket.metric['atr'] > self.stop_loss:
            self.stop_loss = bucket.close + 4*bucket.metric['atr']
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
            return PatternAction.EXIT_TRADE, bucket.close

        return PatternAction.HOLD, None


class PumpReversion(PatternChecker):
    open_positions = []
    status = PatternAction.WAIT
    stop_loss = 0
    low_point = None
    high_point = None
    THRESHOLD = 1.01
    MAX_TIME = timedelta(days=3.0)
    RETEST_TIME = timedelta(minutes=20.0)
    current_stage = None

    PRE_PUMP = 'PRE_PUMP'
    PUMP = 'PUMP'
    POST_PUMP = 'POST_PUMP'
    IN_TRADE = 'IN_TRADE'

    def check_entry(self, index):
        if self.status not in [PatternAction.WAIT]:
            return self.status, None

        # If gain from last bear candle close to the final bull candle close is greater than THRESHOLD percent.
        # If the price re-tests the close of the final bull candle within RETEST_TIME minutes
        # Go short
        # Exit when the price hits the mid way point of the bull run
        # Bail if the target isn't hit within MAX_TIME days

        # TODO: See about performance of entering on the first bearish candle if the close is still greater
        # TODO: than the threshold

        # TODO: Test various exit strategies.  Set Stop Loss at specific price.  Wait till price closes
        # TODO: below target then sell immediately.  Once price drops below target, sell on first bull
        # TODO: candle.  Set Stop Loss at full retrace mark, but also exit on first bull after price
        # TODO: drops below target.

        self.stop_loss = 0
        bucket = self.chart[index]
        time_string = datetime.utcfromtimestamp(index/1000).isoformat()

        if self.current_stage is None:
            self.low_point = min(bucket.open, bucket.close)
            self.current_stage = self.PRE_PUMP

        if self.current_stage == self.PRE_PUMP:
            if bucket.close <= bucket.open:
                # price is still dropping, we're not in a bull run yet
                self.low_point = bucket.close
                return PatternAction.WAIT, None
            else:
                # a bull run starts immediately after a bear candle
                self.current_stage = self.PUMP

        if self.current_stage == self.PUMP:
            if bucket.open <= bucket.close:
                # price is still going up, we're not done with the bull run yet
                self.high_point = bucket.close
                return PatternAction.WAIT, None
            else:
                # the price peaked.
                if self.high_point / self.low_point < self.THRESHOLD:
                    # didn't pump enough, reset
                    self.low_point = bucket.close
                    self.high_point = None
                    self.current_stage = self.PRE_PUMP
                    return PatternAction.WAIT, None
                else:
                    new_opportunity = {'low_point': self.low_point,
                                       'high_point': self.high_point,
                                       'retest_wait': datetime.utcnow() + self.RETEST_TIME,
                                       'max_open_time': datetime.utcnow() + self.MAX_TIME,
                                       'entry_price': self.high_point,
                                       'exit_price': (self.high_point + self.low_point) / 2.0,
                                       'stage': self.POST_PUMP
                                       }
                    self.open_positions.append(new_opportunity)
                    self.current_stage = self.PRE_PUMP
                    # reset high/low point in case we start a second pump really soon
                    self.low_point = bucket.close
                    self.high_point = None
                    return PatternAction.GO_SHORT, new_opportunity

        return PatternAction.WAIT, None


    def check_exit(self, index):

        # TODO: Exit immediately or drop stop loss to a break even/slight win if lower time frame
        # TODO  jumps fast against you

        if self.status not in [PatternAction.HOLD]:
            return self.status, None

        bucket = self.chart[index]
        trades_to_exit = []
        remaining_trades = []

        for trade in self.open_positions:
            if trade['stage'] == self.IN_TRADE:
                if bucket.low < trade['exit_price']:
                    trades_to_exit.append(trade)
                elif trade['max_open_time'] < datetime.utcnow():
                    trades_to_exit.append(trade)
                else:
                    remaining_trades.append(trade)
            elif trade['stage'] == self.POST_PUMP:
                if trade['retest_wait'] > datetime.utcnow():
                    # keep the trade in case it re-tests the high point of the pump
                    remaining_trades.append(trade)

        self.open_positions = remaining_trades

        if trades_to_exit:
            return PatternAction.EXIT_TRADE, trades_to_exit

        return PatternAction.HOLD, None


class SimplePumpReversion(PatternChecker):
    open_positions = {}
    BUY_THRESHOLD = 1.05
    SELL_THRESHOLD = 0.50
    STOP_THRESHOLD = 0.95
    MAX_TIME = timedelta(days=5.0)

    def check_entry(self, index):
        bucket = self.chart[index]
        time_string = datetime.utcfromtimestamp(index/1000).isoformat()

        # Entry:
        # If:
        # - Current candle is Bearish *AND*
        # - Current candle drop greater than BUY_THRESHOLD percent
        # Then:
        # - Buy one position
        # Exit:
        # If:
        # - Price closes above SELL_THRESHOLD percent of current candle *OR*
        # - Price drops below STOP_PERCENT percent of entry price *OR*
        # - Position has been open for MAX_TIME days

        # Candle is bearish
        if bucket.close >= bucket.open:
            return PatternAction.WAIT, None

        # Candle drop is greater than threshold
        if bucket.open / bucket.close < self.BUY_THRESHOLD:
            return  PatternAction.WAIT, None

        new_opportunity = {'target_entry_price': bucket.close,
                           'target_exit_price': (bucket.open - bucket.close) * self.SELL_THRESHOLD + bucket.close,
                           'stop_loss': bucket.close * self.STOP_THRESHOLD,
                           'max_open_time': datetime.utcnow() + self.MAX_TIME
                           }
        print('New Opportunity: {entry} {exit} {stop} {end}'.format(entry=new_opportunity['target_entry_price'],
                                                                    exit=new_opportunity['target_exit_price'],
                                                                    stop=new_opportunity['stop_loss'],
                                                                    end=new_opportunity['max_open_time']))
        self.open_positions[index] = new_opportunity
        return PatternAction.GO_LONG, new_opportunity


    def check_exit(self, index):

        # Exit:
        # If:
        # - Price closes above SELL_THRESHOLD percent of current candle *OR*
        # - Price drops below STOP_THRESHOLD percent of entry price *OR*
        # - Position has been open for MAX_TIME days

        bucket = self.chart[index]
        trades_to_exit = {}
        remaining_trades = {}

        for index, trade in self.open_positions.items():
            if bucket.close > trade['target_exit_price']:
                trade['actual_exit_price'] = bucket.close
                trades_to_exit[index] = trade
            elif bucket.close < trade['stop_loss']:
                trade['actual_exit_price'] = bucket.close
                trades_to_exit[index] = trade
            elif trade['max_open_time'] < datetime.utcnow():
                trade['actual_exit_price'] = bucket.close
                trades_to_exit[index] = trade
            else:
                remaining_trades[index] = trade

        self.open_positions = remaining_trades

        if trades_to_exit:
            return PatternAction.EXIT_TRADE, trades_to_exit

        return PatternAction.HOLD, None


class SimpleMeanReversion(PatternChecker):
    previous_candle_bullish = False
    open_positions = {}

    def check_entry(self, index):
        bucket = self.chart[index]
        time_string = datetime.utcfromtimestamp(index/1000).isoformat()

        # Entry:
        # If:
        # - Current candle is Bearish *AND*
        # - Previous candle was Bullish *AND*
        # - Delta EMA 10 is positive
        # Then:
        # - Buy one position
        # Exit:
        # - First Bullish candle

        # Candle is bearish
        if bucket.close >= bucket.open:
            self.previous_candle_bullish = True
            return PatternAction.WAIT, None

        if not self.previous_candle_bullish:
            return PatternAction.WAIT, None
        self.previous_candle_bullish = False

        # Trending up
        if bucket.metric['ema10_delta'] <= 0:
            return PatternAction.WAIT, None

        if bucket.metric['ema10'] < bucket.metric['ema25']:
            return PatternAction.WAIT, None

        # one position at a time
        if len(self.open_positions) > 0:
            return PatternAction.WAIT, None

        new_opportunity = {'target_entry_price': bucket.close}
        print('New Opportunity: {entry}'.format(entry=new_opportunity['target_entry_price']))
        self.open_positions[index] = new_opportunity
        return PatternAction.GO_LONG, new_opportunity


    def check_exit(self, index):

        # Exit:
        # - Next candle

        bucket = self.chart[index]
        trades_to_exit = {}
        remaining_trades = {}

        for trade_index, trade in self.open_positions.items():
            if trade_index != index:
                trades_to_exit[trade_index] = trade
                trade['actual_exit_price'] = bucket.close
            else:
                remaining_trades[trade_index] = trade
        self.open_positions = remaining_trades

        if trades_to_exit:
            return PatternAction.EXIT_TRADE, trades_to_exit

        return PatternAction.HOLD, None


