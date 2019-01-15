import requests
import datetime
import traceback
from binance import exceptions
from bin_trend import BinanceTrend

if __name__ == "__main__":
    try:
        bt = BinanceTrend()
        bt.start_logging()
        bt.query_coin_balances()
        bt.quick_exit()
    except exceptions.BinanceAPIException as e:
        bt.exception_logger.error('Time: {}'.format(datetime.utcnow().isoformat()))
        bt.exception_logger.error(traceback.format_exc())
        if e.code == -1021:
            bt.exception_logger.info('Timestamp error, pausing and trying again')
            traceback.print_exc()
            print('Timestamp error code: ', e)
            print('Pausing and trying again')
        elif e.code == -1001:
            bt.exception_logger.error('Disconnect error')
            traceback.print_exc()
            print('Disconnected', e)
        elif e.code == -2010:
            # insufficient funds.  this should never happen if we have accurate
            # values for our coin balances.  try restarting just about everything
            bt.exception_logger.error('Time: {}'.format(datetime.utcnow().isoformat()))
            bt.exception_logger.error('Exception placing an order, insufficient funds')
            bt.exception_logger.error('{} Funds: {} {}'.format(bt.COIN1,
                                                               str(bt.balance_book[bt.COIN1]['free']),
                                                               str(bt.balance_book[bt.COIN1]['locked'])))
            bt.exception_logger.error('{} Funds: {} {}'.format(bt.COIN2,
                                                               str(bt.balance_book[bt.COIN2]['free']),
                                                               str(bt.balance_book[bt.COIN1]['locked'])))
            bt.exception_logger.error(traceback.format_exc())
            traceback.print_exc()
            print('Insufficient funds Exception placing order', e)
        else:
            traceback.print_exc()
    except requests.exceptions.ReadTimeout as e:
        bt.exception_logger.error('Time: {}'.format(datetime.utcnow().isoformat()))
        bt.exception_logger.error('Disconnect error, pausing and reconnecting')
        bt.exception_logger.error(traceback.format_exc())
        traceback.print_exc()
        print('Disconnected', e)
    except Exception as e:
        print('Exitting on exception: ', e)
        bt.exception_logger.error('Time: {}'.format(datetime.utcnow().isoformat()))
        bt.exception_logger.error(traceback.format_exc())
        traceback.print_exc()


