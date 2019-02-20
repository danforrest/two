import requests
from datetime import datetime, date
import traceback
from binance import exceptions
from binance.client import Client
from bin_trend import BinanceTrend
from candlestick_chart import CandleStickChart
import pytz

if __name__ == "__main__":
    try:
        start_date = datetime(year=2017, month=10, day=1, tzinfo=pytz.utc)
        end_date = datetime(year=2019, month=1, day=31, tzinfo=pytz.utc)
        print('Initialize client')
        bt = BinanceTrend()
        pair = 'ETHUSDT'
        interval = Client.KLINE_INTERVAL_1MINUTE
        candlesticks = {pair: CandleStickChart(pair, interval)}
        print('Gather historical data')
        klines = bt.client.get_historical_klines(pair, interval, start_date.isoformat(), end_date.isoformat())
        print('Organize historical data')
        for candlestick in klines:
            start_time = int(candlestick[0])
            if start_time not in candlesticks[pair].chart_data:
                candlesticks[pair].add_match(symbol=pair,
                                             interval=interval,
                                             start_time=int(candlestick[0]),
                                             open=float(candlestick[1]),
                                             high=float(candlestick[2]),
                                             low=float(candlestick[3]),
                                             close=float(candlestick[4]),
                                             volume=float(candlestick[5]),
                                             finished=True)

        print('Save historical data')
        print('count: {}'.format(len(candlesticks[pair].chart_data)))
        candlesticks[pair].export_json_by_month()

    except exceptions.BinanceAPIException as e:
        if e.code == -1021:
            traceback.print_exc()
            print('Timestamp error code: ', e)
        elif e.code == -1001:
            traceback.print_exc()
            print('Disconnect error', e)
        else:
            traceback.print_exc()
    except requests.exceptions.ReadTimeout as e:
        traceback.print_exc()
        print('Disconnect error', e)
    except Exception as e:
        print('Exitting on exception: ', e)
        traceback.print_exc()


