from datetime import datetime
from enum import Enum


class PatternAction(str, Enum):
    WAIT = 'ACTION_WAIT'
    GO_LONG = 'ACTION_LONG'
    HOLD = 'ACTION_HOLD'
    GO_SHORT = 'ACTION_SHORT'
    EXIT_TRADE = 'ACTION_EXIT_TRADE'


class PatternChecker:

    def __init__(self, chart):
        self.chart = chart

    def check_entry(self, index):
        pass

    def check_exit(self, index):
        pass


class BullCryptoMovingAverageChecker(PatternChecker):
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

        # 5 EMA is above 25 EMA
        if bucket.metric['ema5'] <= bucket.metric['ema25']:
            print(time_string, 'ema5: ', bucket.metric['ema5'], bucket.metric['ema25'])
            return PatternAction.WAIT, None

        # price open is above 25 EMA
        if bucket.open <= bucket.metric['ema25']:
            print(time_string, 'open: ', bucket.open, bucket.metric['ema25'])
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
        print('ema5 delta {}'.format(bucket.metric['ema5_delta']))
        print('ema100 {}'.format(bucket.metric['ema100']))
        print('ema50 {}'.format(bucket.metric['ema50']))
        print('ema25 {}'.format(bucket.metric['ema25']))
        print('ema5 {}'.format(bucket.metric['ema5']))
        print('open {}'.format(bucket.open))
        print('close {}'.format(bucket.close))
        print('atr {}'.format(bucket.metric['atr']))
        self.status = PatternAction.GO_LONG
        # self.stop_loss = bucket.metric['ema100']
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

        # self.soft_stop = max(self.stop_loss, bucket.metric['ema100'])
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


class BearCryptoMovingAverageChecker(PatternChecker):
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

        # 5 EMA is below 25 EMA
        if bucket.metric['ema5'] >= bucket.metric['ema25']:
            print(time_string, 'ema5: ', bucket.metric['ema5'], bucket.metric['ema25'])
            return PatternAction.WAIT, None

        # price open is below 25 EMA
        if bucket.open >= bucket.metric['ema25']:
            print(time_string, 'open: ', bucket.open, bucket.metric['ema25'])
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
        print('ema5 delta {}'.format(bucket.metric['ema5_delta']))
        print('ema100 {}'.format(bucket.metric['ema100']))
        print('ema50 {}'.format(bucket.metric['ema50']))
        print('ema25 {}'.format(bucket.metric['ema25']))
        print('ema5 {}'.format(bucket.metric['ema5']))
        print('open {}'.format(bucket.open))
        print('close {}'.format(bucket.close))
        print('atr {}'.format(bucket.metric['atr']))
        self.status = PatternAction.GO_SHORT
        # self.stop_loss = bucket.metric['ema100']
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

        # self.soft_stop = min(self.stop_loss, bucket.metric['ema100'])
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

