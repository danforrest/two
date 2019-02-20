from datetime import datetime, date
import json
import plotly
import plotly.graph_objs as go
from queue import Queue, PriorityQueue
import sys

ONE_MINUTE = 60
THREE_MINUTES = 3 * ONE_MINUTE
FIVE_MINUTES = 5 * ONE_MINUTE
FIFTEEN_MINUTES = 15 * ONE_MINUTE
ONE_HOUR = 60 * ONE_MINUTE
FOUR_HOURS = 4 * ONE_HOUR
ONE_DAY = 24 * ONE_HOUR

ALPHA_10 = 2.0 / 11.0
ALPHA_12 = 2.0 / 13.0
ALPHA_15 = 2.0 / 16.0
ALPHA_20 = 2.0 / 21.0
ALPHA_26 = 2.0 / 27.0
ALPHA_50 = 2.0 / 51.0
ALPHA_100 = 2.0 / 101.0
ALPHA_200 = 2.0 / 201.0

ATR_LENGTH = 14

EMA_PERIODS = [5, 10, 12, 15, 20, 25, 26, 50, 100, 200]

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


class CandleStickDataPoint:
    open = 0
    close = 0
    high = 0
    low = sys.float_info.max
    total_volume = 0
    time_index = 0
    finished = False
    metric = None

    def __init__(self, time_index):
        self.metric = {}
        self.time_index = time_index

    def __repr__(self):
        return self.to_json();

    def to_csv(self):
        #return_value = datetime.utcfromtimestamp(self.time_index).isoformat()
        return_value = str(self.time_index)
        return_value += ',' + str(self.open)
        return_value += ',' + str(self.close)
        return_value += ',' + str(self.high)
        return_value += ',' + str(self.low)
        return_value += ',' + str(self.total_volume)
        metric_names = sorted(self.metric.keys())
        for name in metric_names:
            return_value += ',' + str(self.metric[name])
        return_value += '\n'
        return return_value

    def csv_headers(self):
        return_value = 'Date, Open, Close, High, Low, Volume'
        metric_names = sorted(self.metric.keys())
        for name in metric_names:
            return_value += ', ' + name
        return_value += '\n'
        return return_value

    def to_json(self):
        # return_value = {'time_index': datetime.utcfromtimestamp(self.time_index).timestamp(),
        return_value = {'time_index': self.time_index,
                        'open': self.open,
                        'close': self.close,
                        'high': self.high,
                        'low': self.low,
                        'total_volume': self.total_volume,
                        'finished': self.finished}#,
                        # 'metric': json.dumps(self.metric)}
        return return_value

    def short_string(self):
        return '{time} {open} {close}'.format(time=self.time_index, open=self.open, close=self.close)


class CandleStickChart:
    chart_data = {}
    start_time = None
    name = 'None'
    metric_index = None

    def __init__(self, name, interval):
        self.name = name
        self.interval = interval
        self.to_be_processed = PriorityQueue()
        self.metric_to_be_processed = PriorityQueue()

    def export_csv(self, csv_file_name='data.csv'):
        csv_file = open(csv_file_name, 'w')
        csv_text = self.chart_data['1m'][0].csv_headers()
        for point in self.chart_data['1m']:
            if point is not None:
                csv_text += point.to_csv()
        csv_file.write(csv_text)
        csv_file.write('\n')
        csv_file.close()

    def export_json(self, json_file_name='data.json', data=None):
        json_file = open(json_file_name, 'w')
        json_data = {}
        for timestamp, data_point in data.items():
            json_data[timestamp] = data_point.to_json()
        json_file.write(json.dumps(json_data))
        json_file.write('\n')
        json_file.close()

    def export_json_by_month(self):
        monthly_points = {}
        for timestamp, candlestick in self.chart_data.items():
            month = '{date:%Y}_{date:%m}'.format(date=datetime.utcfromtimestamp(timestamp/1000))
            if month not in monthly_points:
                monthly_points[month] = {}
            monthly_points[month][timestamp] = candlestick
        for month, data in monthly_points.items():
            filename = '{dir}\\{pair}_{interval}_{month}_candlesticks.json'.format(dir='data',
                                                                                   pair=self.name,
                                                                                   interval=self.interval,
                                                                                   month=month)
            self.export_json(filename, data)

    def export_json_string(self):
        point_json_list = []
        for point in self.chart_data['5m']:
            if point is None:
                point_json_list.append(None)
            else:
                point_json_list.append(point.to_json())
        return json.dumps(point_json_list)

    def import_json(self, json_file_name='data.json'):
        try:
            json_file = json.load(open(json_file_name, 'r'))
        except FileNotFoundError:
            print('file: {} does not exist'.format(json_file_name))
            return False
        for timestamp, data in json_file.items():
            timestamp = int(timestamp)
            point = CandleStickDataPoint(timestamp)
            point.open = data['open']
            point.close = data['close']
            point.high = data['high']
            point.low = data['low']
            point.total_volume = data['total_volume']
            point.finished = data['finished']
            self.chart_data[timestamp] = point
            if point.finished:
                self.metric_to_be_processed.put(timestamp)
        return True

    def export_plotly(self, length):
        date_list = []
        open_list = []
        high_list = []
        low_list = []
        close_list = []

        for bucket in self.chart_data['1m'][-1*length:]:
        # for bucket in self.one_minute_chart[-1*length:]:
            date_list.append(datetime.fromtimestamp(bucket.time_index).isoformat())
            open_list.append(bucket.open)
            high_list.append(bucket.high)
            low_list.append(bucket.low)
            close_list.append(bucket.close)

        trace = go.Candlestick(x=date_list,
                               open=open_list,
                               high=high_list,
                               low=low_list,
                               close=close_list)
        data = [trace]
        plotly.offline.plot(data)

    def short_print(self):
        for candlestick in self.chart_data.values():
            print('{candlestick}'.format(candlestick=candlestick.short_string()))

    def add_match(self, symbol, interval, start_time, open, close, high, low, volume, finished=False):
        if symbol != self.name:
            print('wrong data')
            # wrong data
            return False
        if interval != self.interval:
            # wrong interval
            return False
        if start_time in self.chart_data:
            current_point = self.chart_data[start_time]
        else:
            current_point = CandleStickDataPoint(start_time)
            self.chart_data[start_time] = current_point
        current_point.open = open
        current_point.close = close
        current_point.high = high
        current_point.low = low
        current_point.total_volume = volume
        current_point.finished = finished

        if self.start_time is None:
            self.start_time = start_time

        if finished:
            self.metric_to_be_processed.put(start_time)
        return True

    def update_metrics(self):
        previous_point = self.metric_index
        current_point = None
        while not self.metric_to_be_processed.empty():
            current_point = self.metric_to_be_processed.get()
            self.update_ema(current_point, previous_point)
            self.update_atr(current_point, previous_point)
            previous_point = current_point
            self.to_be_processed.put(current_point)

        self.metric_index = current_point


    def recalc_all_metrics(self):
        while not self.metric_to_be_processed.empty():
            self.metric_to_be_processed.get(False)
        while not self.to_be_processed.empty():
            self.to_be_processed.get(False)
        for start_time in self.chart_data:
            if self.chart_data[start_time].finished:
                self.metric_to_be_processed.put(start_time)
        self.metric_index = None
        self.update_metrics()

    def update_ema(self, index, previous_index):
        # add the 20 point exponential moving average to buckets
        bucket = self.chart_data[index]

        for period in EMA_PERIODS:
            ema_name = 'ema' + str(period)
            vol_name = 'vol_' + ema_name
            ema_delta = ema_name + '_delta'
            if previous_index is None:
                bucket.metric[ema_name] = bucket.close
                bucket.metric[ema_delta] = 0
                bucket.metric[vol_name] = bucket.total_volume
            else:
                previous_bucket = self.chart_data[previous_index]
                alpha = 2.0 / (period + 1)

                ema_previous = previous_bucket.metric[ema_name]
                bucket.metric[ema_name] = ema_previous + alpha * (bucket.close - ema_previous)
                # Add delta_ema to find up/down trends
                bucket.metric[ema_delta] = bucket.metric[ema_name] - ema_previous
                #print('time: ', i, chart[i].close, chart[i].metric['ema10'], chart[i].metric['ema20'], chart[i].metric['ema50'], chart[i].metric['ema100'], chart[i].metric['ema200'])
                # Add volume ema to find pump/dump trends
                vol_previous = previous_bucket.metric[vol_name]
                bucket.metric[vol_name] = vol_previous + alpha * (bucket.total_volume - vol_previous)

        if 12 in EMA_PERIODS and 26 in EMA_PERIODS:
            if previous_index is None:
                bucket.metric['macd_histogram'] = bucket.metric['ema12'] - bucket.metric['ema26']
                bucket.metric['macd_delta'] = 0
            else:
                previous_bucket = self.chart_data[previous_index]
                macd_previous = previous_bucket.metric['macd_histogram']
                bucket.metric['macd_histogram'] = bucket.metric['ema12'] - bucket.metric['ema26']
                bucket.metric['macd_delta'] = bucket.metric['macd_histogram'] - macd_previous

    def update_atr(self, index, previous_index):
        bucket = self.chart_data[index]
        if previous_index is None:
            bucket.metric['atr'] = bucket.high - bucket.low
        else:
            previous_bucket = self.chart_data[previous_index]
            range_max = max(bucket.high, previous_bucket.close)
            range_min = min(bucket.low, previous_bucket.close)
            bucket.metric['atr'] = ((previous_bucket.metric['atr'] * (ATR_LENGTH-1)) + (range_max - range_min)) / ATR_LENGTH

    def set_range_minmax(self, chart, range_length):
        def alt_if_none(value, alt):
            return alt if value is None else value
        min_name = 'range' + str(range_length) + '_min'
        max_name = 'range' + str(range_length) + '_max'
        range_min = [alt_if_none(getattr(x, 'low', None), sys.float_info.max) for x in chart]
        range_max = [alt_if_none(getattr(x, 'high', None), 0) for x in chart]

        current_min = range_min[0]
        current_max = range_max[0]
        chart[0].metric[min_name] = current_min
        chart[0].metric[max_name] = current_max
        for i in range(1, range_length):
            current_min = min(current_min, range_min[i])
            current_max = max(current_max, range_max[i])
            if chart[i] is not None:
                chart[i].metric[min_name] = current_min
                chart[i].metric[max_name] = current_max
        for i in range(range_length, len(chart)):
            chart[i].metric[min_name] = min(range_min[i-(range_length-1):i+1])
            chart[i].metric[max_name] = max(range_max[i-(range_length-1):i+1])

    def set_rsi(self, chart, range_length):
        def calc_rsi(avg_gain, avg_loss):
            if avg_gain == 0:
                # simple calculation.
                return 0
            elif avg_loss == 0:
                # avoid divide by zero
                return 100
            else:
                # average loss is negative => RS is negative => need to subtract from 1
                # instead of adding to 1.
                return 100 - (100 / (1 - (avg_gain / avg_loss)))

        rsi_name = 'rsi' + str(range_length)
        average_gain = 0
        average_loss = 0

        if len(chart) < range_length:
            for index in range(0, len(chart)):
                chart[index].metric[rsi_name] = 50
            return

        chart[0].metric[rsi_name] = 50
        for index in range(1, range_length):
            close_diff = chart[index].close - chart[index-1].close
            if close_diff > 0:
                average_gain += close_diff
            elif close_diff < 0:
                # average loss will be negative to make the computation more efficient
                average_loss += close_diff
            chart[index].metric[rsi_name] = 50
        average_gain /= range_length
        average_loss /= range_length
        chart[range_length-1].metric[rsi_name] = calc_rsi(average_gain, average_loss)

        for index in range(range_length, len(chart)):
            close_diff = chart[index].close - chart[index-1].close
            if close_diff > 0:
                average_gain = ((average_gain*(range_length-1)) + close_diff) / range_length
                average_loss = ((average_loss*(range_length-1)) + 0) / range_length
            elif close_diff < 0:
                # average loss will be negative to make the computation more efficient
                average_gain = ((average_gain*(range_length-1)) + 0) / range_length
                average_loss = ((average_loss*(range_length-1)) + close_diff) / range_length
            else:
                average_gain = ((average_gain*(range_length-1)) + 0) / range_length
                average_loss = ((average_loss*(range_length-1)) + 0) / range_length
            chart[index].metric[rsi_name] = calc_rsi(average_gain, average_loss)
            # Is this really correct?
#            print('time: ', chart[index].time_index, 'rsi: ', chart[index].metric[rsi_name])

    def set_stochastic_oscillator(self, chart, range_length, ma_length):

        stochastic_name = 'stochastic' + str(range_length) + '_oscillator'
        stochastic_ma_name = 'stochastic' + str(range_length) + '_ma' + str(ma_length)
        range_min_name = 'range' + str(range_length) + '_min'
        range_max_name = 'range' + str(range_length) + '_max'

        if len(chart) < range_length:
            for index in range(0, len(chart)):
                chart[index].metric[stochastic_name] = 50
                chart[index].metric[stochastic_ma_name] = 50
            return

        for index in range(0, range_length):
            chart[index].metric[stochastic_name] = 50
            chart[index].metric[stochastic_ma_name] = 50

        for index in range(range_length, len(chart)):
            period_range = chart[index].metric[range_max_name] - chart[index].metric[range_min_name]
            K = 100 * ((chart[index].close - chart[index].metric[range_min_name]) / period_range)
            # why isn't K between 0 and 100???
            chart[index].metric[stochastic_name] = K
            chart[index].metric[stochastic_ma_name] = ((chart[index-1].metric[stochastic_ma_name]*(ma_length-1))+K)/ma_length
            # if K > 100 and index == 19997:
            # print('chart: ', index, ' ', chart[index].close, ' ', chart[index].high, ' ', chart[index].low, ' ',
            #       chart[index].metric)
            # print('max: ', chart[index].metric[range_max_name], 'min: ', chart[index].metric[range_min_name])
            # print('time: ', datetime.utcfromtimestamp(chart[index].time_index).isoformat(), 'stochastic: ', chart[index].metric[stochastic_name])

    def set_metrics(self):
        # initialize the charts
        self.update_metrics()
        self.set_range_minmax(self.chart_data, 20)
        self.set_range_minmax(self.chart_data, 14)
        self.set_stochastic_oscillator(self.chart_data, 14, 3)
        self.set_rsi(self.chart_data, 14)


