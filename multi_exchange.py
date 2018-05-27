from binance_exchange import BinanceExchange
from kucoin_exchange import KucoinExchange
from cryptopia_exchange import CryptopiaExchange
import sys
import time
import logging
from datetime import datetime, timedelta
import requests
import traceback
import gc
import copy
from exchange import Exchange, COIN_LIST, PAIR_LIST, CYCLE_LIST, PAIR_COINS
from exchange import BTC, ETH, NEO, LTC, USDT
from exchange import BTCUSDT, ETHBTC, ETHUSDT, NEOBTC, NEOETH, NEOUSDT, LTCUSDT, LTCBTC, LTCETH

ONE_DAY = timedelta(days=1)
log_start_time = None
order_logger = None

class LogEntry():
    version = '1.0'
    name = 'multi_exchange_simple_arbitrage'
    timestamp = None
    pair = None
    bid_exchange = None
    ask_exchange = None
    expected_return = 0.0
    best_bid = 0.0
    best_ask = 0.0
    best_bid_adjusted = 0.0
    best_ask_adjusted = 0.0
    best_bid_size = 0.0
    best_ask_size = 0.0
    actual_bid = [0.0]
    actual_ask = [0.0]
    actual_bid_size = [0.0]
    actual_ask_size = [0.0]
    bid_fee = {}
    ask_fee = {}
    total_return = {}
    bids = {}
    asks = {}
    start_balances = {}
    delta_balances = {}
    coin1 = None
    coin2 = None
    bid_order = None
    ask_order = None


    def to_string(self):
        return_value = [self.name, self.version, self.timestamp.isoformat(), self.pair,
                        self.bid_exchange.name, self.ask_exchange.name, self.expected_return,
                        {'best_bid': self.best_bid, 'best_ask': self.best_ask,
                         'best_bid_adjusted': self.best_bid_adjusted, 'best_ask_adjusted': self.best_ask_adjusted,
                         'best_bid_size': self.best_bid_size, 'best_ask_size': self.best_ask_size},
                        {'actual_bid': self.actual_bid, 'actual_ask': self.actual_ask,
                         'actual_bid_size': self.actual_bid_size, 'actual_ask_size': self.actual_ask_size,
                         'bid_fee': self.bid_fee, 'ask_fee': self.ask_fee, 'total_return': self.total_return},
                        {'bids': self.bids, 'asks': self.asks,
                         'start_balances': self.start_balances, 'delta_balances': self.delta_balances},
                        self.coin1, self.coin2, self.bid_order, self.ask_order]
        return str(return_value)


    def get_latest_order_data(self):
        if self.bid_order is not None:
            self.bid_exchange.update_order(self.bid_order)
            self.actual_bid = self.bid_order['somePriceName?']
            self.actual_bid_size = self.bid_order['someSizeName?']
            self.bid_fee = self.bid_order['someFeeName?']
            # updated self.delta_balances
        if self.ask_order is not None:
            self.ask_exchange.update_order(self.ask_order)
            self.actual_ask = self.ask_order['somePriceName?']
            self.actual_ask_size = self.ask_order['someSizeName?']
            self.ask_fee = self.ask_order['someFeeName?']
            # updated self.delta_balances


b_balances = {BTC: 0.0625,
              ETH: 0.715,
              LTC: 2.9,
              NEO: 6.5,
              USDT: 500.0}
k_balances = {BTC: 0.0625,
              ETH: 0.715,
              LTC: 2.9,
              NEO: 6.5,
              USDT: 500.0}
c_balances = {BTC: 0.0625,
              ETH: 0.715,
              LTC: 2.9,
              NEO: 6.5,
              USDT: 500.0}
a_balances = {BTC: 0.125,
              ETH: 1.43,
              LTC: 5.8,
              NEO: 13,
              USDT: 1000.0}
fees = {BTC: 0.0,
        ETH: 0.0,
        LTC: 0.0,
        NEO: 0.0,
        USDT: 0.0}

total_return = {BTC: 0.0,
                ETH: 0.0,
                LTC: 0.0,
                NEO: 0.0,
                USDT: 0.0}

price = {BTC: 8000.0,
         ETH: 700.0,
         LTC: 150.0,
         NEO: 60.0,
         USDT: 1.0}

START_TIME = datetime.utcnow()

def calculate_raw_coin_ratio(binance_raw_order_book, kucoin_raw_order_book, cryptopia_raw_order_book, coin1, coin2):
    if coin1 + coin2 in PAIR_LIST:
        pair = coin1+coin2
        coin1_per_coin2 = 1 / min(binance_raw_order_book[pair].ask*(1+binance_exchange.FEE),
                                  kucoin_raw_order_book[pair].ask*(1+kucoin_exchange.FEE),
                                  cryptopia_raw_order_book[pair].ask*(1+cryptopia_exchange.FEE))
        coin2_per_coin1 = max(binance_raw_order_book[pair].bid*(1-binance_exchange.FEE),
                              kucoin_raw_order_book[pair].bid*(1-kucoin_exchange.FEE),
                              cryptopia_raw_order_book[pair].bid*(1-cryptopia_exchange.FEE))
    elif coin2 + coin1 in PAIR_LIST:
        pair = coin2+coin1
        coin2_per_coin1 = 1 / min(binance_raw_order_book[pair].ask*(1+binance_exchange.FEE),
                                  kucoin_raw_order_book[pair].ask*(1+kucoin_exchange.FEE),
                                  cryptopia_raw_order_book[pair].ask*(1+cryptopia_exchange.FEE))
        coin1_per_coin2 = max(binance_raw_order_book[pair].bid*(1-binance_exchange.FEE),
                              kucoin_raw_order_book[pair].bid*(1-kucoin_exchange.FEE),
                              cryptopia_raw_order_book[pair].bid*(1-cryptopia_exchange.FEE))
    else:
        error_string = 'No pairs found for coins', coin1, coin2, 'in: ', PAIR_LIST
        print(error_string)
        raise Exception(error_string)

    return coin1_per_coin2, coin2_per_coin1


def run_arbitrage(binance_exchange, kucoin_exchange, cryptopia_exchange):

    print('all prepped')
    global b_balances
    global k_balances
    global c_balances
    global total_return

    # binance_exchange.query_coin_balances()
    # kucoin_exchange.query_coin_balances()
    while True:
        try:

            # for coin in sorted(binance_exchange.balance_book):
            #     if coin not in a_balances:
            #         continue
            #     print('{:<4}  {:>4.8f}  {:>4.8f}  {:>4.8f}'.format(coin,
            #                                                        binance_exchange.balance_book[coin].free,
            #                                                        kucoin_exchange.balance_book[coin].free,
            #                                                        binance_exchange.balance_book[coin].free + kucoin_exchange.balance_book[coin].free))
            # print('******************************************')

            start_balance = {'binance': {coin: binance_exchange.balance_book[coin].free for coin in binance_exchange.balance_book if coin in COIN_LIST},
                             'kucoin': {coin: kucoin_exchange.balance_book[coin].free for coin in kucoin_exchange.balance_book if coin in COIN_LIST}}
            kucoint_start_time = datetime.utcnow()
            kucoin_exchange.update_raw_order_book()
            kucoin_end_time = datetime.utcnow()
            # cryptopia_exchange.update_raw_order_book()
            binance_exchange.update_raw_order_book()
            binance_end_time = datetime.utcnow()
            delta1 = kucoin_end_time - kucoint_start_time
            delta2 = binance_end_time - kucoin_end_time
            print('Kucoin time: {} Binance time: {}'.format(delta1, delta2))

            log_entries = []
            # replace the following with the preceding when we start using actual balances
            # Check for simple arbitrage
            for pair in PAIR_LIST:
                #cryptopia_exchange.update_raw_order_pair(pair)
                # binance_exchange.update_raw_order_pair(pair)
                # kucoin_exchange.update_raw_order_pair(pair)
                b_bid = binance_exchange.raw_order_book[pair].bid
                b_ask = binance_exchange.raw_order_book[pair].ask
                k_bid = kucoin_exchange.raw_order_book[pair].bid
                k_ask = kucoin_exchange.raw_order_book[pair].ask
                print('Pair: {} B-Bid: {} K-Bid: {} B-Ask: {} K-Ask: {}'.format(pair, b_bid, k_bid, b_ask, k_ask))
                b_bid_fee = b_bid * (1-binance_exchange.FEE)
                b_ask_fee = b_ask * (1+binance_exchange.FEE)
                k_bid_fee = k_bid * (1-kucoin_exchange.FEE)
                k_ask_fee = k_ask * (1+kucoin_exchange.FEE)
                if max(b_bid_fee, k_bid_fee) <= min(b_ask_fee, k_ask_fee):
                    continue

                coin1 = PAIR_COINS[pair][0]
                coin2 = PAIR_COINS[pair][1]
                b_free = {coin1: binance_exchange.balance_book[coin1].free,
                          coin2: binance_exchange.balance_book[coin2].free}
                k_free = {coin1: kucoin_exchange.balance_book[coin1].free,
                          coin2: kucoin_exchange.balance_book[coin2].free}

                if b_bid_fee > k_ask_fee:
                    if b_bid_fee / k_ask_fee < 1.0005 and b_free[coin1] < (k_free[coin1]/2):
                        print('Profit: {} sell binance is too low'.format(b_bid_fee/k_ask_fee))
                        continue
                    b_bid_max = binance_exchange.raw_order_book[pair].bid_size
                    k_ask_max = kucoin_exchange.raw_order_book[pair].ask_size
                    # volume = min(b_free[coin1] * 0.1, b_bid_max, k_ask_max)
                    volume = b_free[coin1] * 0.1
                    if volume > b_bid_max:
                        print('Binance bid quantity reduced from {} to {}'.format(volume, b_bid_max))
                        volume = b_bid_max
                    if volume > k_ask_max:
                        print('Kucoin ask quantity reduced from {} to {}'.format(volume, k_ask_max))
                        volume = k_ask_max
                    if b_free[coin1] < 0.001 * a_balances[coin1]:
                        print('Binance {} balance too small {}'.format(coin1, 0.001 * a_balances[coin1]))
                        continue
                    if k_free[coin2] < volume * k_ask:
                        print('Kucoin {} balance too small {}'.format(coin2, volume * k_ask))
                        continue
                    if volume < binance_exchange.MIN_AMOUNT[pair]:
                        print('Binance {} volume too small {} < {}'.format(coin1, volume, binance_exchange.MIN_AMOUNT[pair]))
                        continue
                    if volume * k_ask < kucoin_exchange.MIN_AMOUNT[pair]:
                        print('Kucoin {} volume too small {} < {}'.format(coin2, volume*k_ask, kucoin_exchange.MIN_AMOUNT[pair]))
                        continue
                    if b_free[coin1] > volume and k_free[coin2] > volume * k_ask:
                        # sell on binance
                        binance_fee = volume * b_bid * binance_exchange.FEE
                        kucoin_fee = volume * kucoin_exchange.FEE
                        binance_exchange.balance_book[coin1].free -= volume
                        binance_exchange.balance_book[coin2].free += (volume * b_bid) * (1 - binance_exchange.FEE)
                        # buy on kucoin
                        kucoin_exchange.balance_book[coin1].free += volume * (1 - kucoin_exchange.FEE)
                        kucoin_exchange.balance_book[coin2].free -= volume * k_ask
                        direction = 'sell binance'

                        bid_order = binance_exchange.market_convert_coins(coin1, coin2, volume)
                        ask_order = kucoin_exchange.market_convert_coins(coin2, coin1, volume * k_ask)
                        fees[coin2] += binance_fee
                        fees[coin1] += kucoin_fee

                        log_entry = LogEntry()
                        log_entry.timestamp = datetime.utcnow()
                        log_entry.coin1 = coin1
                        log_entry.coin2 = coin2
                        log_entry.pair = pair
                        log_entry.best_bid = b_bid
                        log_entry.best_ask = k_ask
                        log_entry.best_bid_adjusted = b_bid_fee
                        log_entry.best_ask_adjusted = k_ask_fee
                        log_entry.best_bid_size = b_bid_max
                        log_entry.best_ask_size = k_ask_max
                        log_entry.expected_return = b_bid_fee / k_ask_fee
                        log_entry.bids = {'binance': b_bid, 'kucoin': k_bid}
                        log_entry.asks = {'binance': b_ask, 'kucoin': k_ask}
                        log_entry.start_balances = {'binance': {coin1: start_balance['binance'][coin1],
                                                                coin2: start_balance['binance'][coin2]},
                                                    'kucoin': {coin1: start_balance['kucoin'][coin1],
                                                               coin2: start_balance['kucoin'][coin2]}}
                        log_entry.delta_balances = {'binance': {coin1: -volume,
                                                                coin2: (volume * b_bid) * (1 - binance_exchange.FEE)},
                                                    'kucoin': {coin1: volume * (1 - kucoin_exchange.FEE),
                                                               coin2: -(volume * k_ask)},
                                                    'all': {coin1: -volume * kucoin_exchange.FEE,
                                                            coin2: ((volume * b_bid) * (1 - binance_exchange.FEE)) - (volume * k_ask)}}
                        log_entry.total_return = {coin1: log_entry.delta_balances['all'][coin1] * price[coin1],
                                                  coin2: log_entry.delta_balances['all'][coin2] * price[coin2],
                                                  'all': (log_entry.delta_balances['all'][coin1] * price[coin1]) +
                                                         (log_entry.delta_balances['all'][coin2] * price[coin2])}
                        log_entry.bid_exchange = binance_exchange
                        log_entry.ask_exchange = kucoin_exchange
                        # log_entry.bid_order = bid_order
                        # log_entry.ask_order = ask_order
                        log_entries.append(log_entry)
                        if log_entry.total_return['all'] < 0:
                            write_log_entries(log_entries)
                            print('negative return!!!!!')
                            # exit(1)
                    else:
                        continue
                    print('profit! {} {}'.format(b_bid_fee / k_ask_fee, direction))
                elif k_bid_fee > b_ask_fee:
                    if k_bid_fee / b_ask_fee < 1.0005 and k_free[coin1] < (b_free[coin1]/2):
                        print('Profit: {} sell kucoin is too low'.format(k_bid_fee/b_ask_fee))
                        continue
                    b_ask_max = binance_exchange.raw_order_book[pair].ask_size
                    k_bid_max = kucoin_exchange.raw_order_book[pair].bid_size
                    # volume = min(k_free[coin1] * 0.1, b_ask_max, k_bid_max)
                    volume = k_free[coin1] * 0.1
                    if volume > b_ask_max:
                        print('Binance ask quantity reduced from {} to {}'.format(volume, b_ask_max))
                        volume = b_ask_max
                    if volume > k_bid_max:
                        print('Kucoin bid quantity reduced from {} to {}'.format(volume, k_bid_max))
                        volume = k_bid_max
                    if k_free[coin1] < 0.001 * a_balances[coin1]:
                        print('Kucoin {} balance too small {}'.format(coin1, 0.001 * a_balances[coin1]))
                        continue
                    if b_free[coin2] < volume * b_ask:
                        print('Binance {} balance too small {}'.format(coin2, volume * b_ask))
                        continue
                    if volume < kucoin_exchange.MIN_AMOUNT[pair]:
                        print('Kucoin {} volume too small {} < {}'.format(coin1, volume, kucoin_exchange.MIN_AMOUNT[pair]))
                        continue
                    if volume * b_ask < binance_exchange.MIN_AMOUNT[pair]:
                        print('Binance {} volume too small {} < {}'.format(coin2, volume * b_ask, binance_exchange.MIN_AMOUNT[pair]))
                        continue
                    if b_free[coin2] > volume * b_ask and k_free[coin1] > volume:
                        # buy on binance
                        binance_fee = volume * binance_exchange.FEE
                        kucoin_fee = volume * k_bid * kucoin_exchange.FEE
                        binance_exchange.balance_book[coin1].free += volume * (1 - binance_exchange.FEE)
                        binance_exchange.balance_book[coin2].free -= volume * b_ask
                        # sell on kucoin
                        kucoin_exchange.balance_book[coin1].free -= volume
                        kucoin_exchange.balance_book[coin2].free += (volume * k_bid) * (1 - kucoin_exchange.FEE)
                        direction = 'sell kucoin'
                        fees[coin1] += binance_fee
                        fees[coin2] += kucoin_fee

                        log_entry = LogEntry()
                        log_entry.timestamp = datetime.utcnow()
                        log_entry.coin1 = coin1
                        log_entry.coin2 = coin2
                        log_entry.pair = pair
                        log_entry.best_bid = k_bid
                        log_entry.best_ask = b_ask
                        log_entry.best_bid_adjusted = k_bid_fee
                        log_entry.best_ask_adjusted = b_ask_fee
                        log_entry.best_bid_size = k_bid_max
                        log_entry.best_ask_size = b_ask_max
                        log_entry.expected_return = k_bid_fee / b_ask_fee
                        log_entry.bids = {'binance': b_bid, 'kucoin': k_bid}
                        log_entry.asks = {'binance': b_ask, 'kucoin': k_ask}
                        log_entry.start_balances = {'binance': {coin1: start_balance['binance'][coin1],
                                                                coin2: start_balance['binance'][coin2]},
                                                    'kucoin': {coin1: start_balance['kucoin'][coin1],
                                                               coin2: start_balance['kucoin'][coin2]}}
                        log_entry.delta_balances = {'binance': {coin1: volume * (1 - binance_exchange.FEE),
                                                                coin2: -(volume * b_ask)},
                                                    'kucoin': {coin1: -volume,
                                                               coin2: (volume * k_bid) * (1 - kucoin_exchange.FEE)},
                                                    'all': {coin1: -volume * binance_exchange.FEE,
                                                            coin2: ((volume * k_bid) * (1 - kucoin_exchange.FEE)) - (volume * b_ask)}}
                        log_entry.total_return = {coin1: log_entry.delta_balances['all'][coin1] * price[coin1],
                                                  coin2: log_entry.delta_balances['all'][coin2] * price[coin2],
                                                  'all': (log_entry.delta_balances['all'][coin1] * price[coin1]) +
                                                         (log_entry.delta_balances['all'][coin2] * price[coin2])}
                        log_entry.bid_exchange = kucoin_exchange
                        log_entry.ask_exchange = binance_exchange
                        # log_entry.bid_order = bid_order
                        # log_entry.ask_order = ask_order
                        log_entries.append(log_entry)
                        if log_entry.total_return['all'] < 0:
                            write_log_entries(log_entries)
                            print('negative return!!!!!')
                            # exit(1)
                    else:
                        continue
                    print('profit! {} {}'.format(k_bid_fee/b_ask_fee, direction))
                #break
            for log_entry in log_entries:
                log_entry.get_latest_order_data()
            write_log_entries(log_entries)
            print('******************************************')
            # print('Coin  Bin Balance Kuc Balance Total       Return      Return Val  Fees')
            print('{:<4} {:<13} {:<13} {:<13} {:<13} {:<13} {:<13}'.format('Coin', 'Bin Balance', 'Kuc Balance',
                                                                           'Total', 'Return', 'Return Val', 'Fees'))
            # binance_exchange.query_coin_balances()
            # kucoin_exchange.query_coin_balances()
            cumulative_return = 0.0
            cumulative_fees = 0.0
            for coin in sorted(binance_exchange.balance_book):
                if coin not in a_balances:
                    continue
                total_return[coin] = (binance_exchange.balance_book[coin].free - b_balances[coin]) + \
                                     (kucoin_exchange.balance_book[coin].free - k_balances[coin])
                print('{:<4} {:>13} {:>13} {:>13} {:>13} {:>13} {:>13}'.format(coin,
                                                                               fmt_float(binance_exchange.balance_book[coin].free),
                                                                               fmt_float(kucoin_exchange.balance_book[coin].free),
                                                                               fmt_float(binance_exchange.balance_book[coin].free + kucoin_exchange.balance_book[coin].free),
                                                                               fmt_float(total_return[coin]),
                                                                               fmt_float(total_return[coin] * price[coin]),
                                                                               fmt_float(fees[coin])))
                cumulative_return += total_return[coin] * price[coin]
                cumulative_fees += fees[coin] * price[coin]
            current_time = datetime.utcnow()
            delta = current_time - START_TIME
            print('Running time: {:<30}                 {:>13} {:>13}'.format(str(delta),
                                                                              fmt_float(cumulative_return),
                                                                              fmt_float(cumulative_fees)))
            print('******************************************')


            continue
            # check for triangle arbitrage
            for coins, pairs in CYCLE_LIST:
                COIN1, COIN2, COIN3 = coins
                # calculate balance of each coin
                coin_per_coin = {COIN1: {COIN2: 0.0, COIN3: 0.0},
                                 COIN2: {COIN1: 0.0, COIN3: 0.0},
                                 COIN3: {COIN1: 0.0, COIN2: 0.0}}

                coin_per_coin[COIN1][COIN2], coin_per_coin[COIN2][COIN1] = calculate_raw_coin_ratio(binance_exchange.raw_order_book,
                                                                                                    kucoin_exchange.raw_order_book,
                                                                                                    COIN1, COIN2)
                coin_per_coin[COIN1][COIN3], coin_per_coin[COIN3][COIN1] = calculate_raw_coin_ratio(binance_exchange.raw_order_book,
                                                                                                    kucoin_exchange.raw_order_book,
                                                                                                    COIN1, COIN3)
                coin_per_coin[COIN2][COIN3], coin_per_coin[COIN3][COIN2] = calculate_raw_coin_ratio(binance_exchange.raw_order_book,
                                                                                                    kucoin_exchange.raw_order_book,
                                                                                                    COIN2, COIN3)

                forward_arbitrage = coin_per_coin[COIN1][COIN3] * coin_per_coin[COIN3][COIN2] * coin_per_coin[COIN2][COIN1]
                reverse_arbitrage = coin_per_coin[COIN1][COIN2] * coin_per_coin[COIN2][COIN3] * coin_per_coin[COIN3][COIN1]
                print('Coins: {} forward: {} reverse: {}'.format(coins, forward_arbitrage, reverse_arbitrage))

            check_logs()

        except Exception as e:
            print('Exitting on exception: ', e)
            raise e

def fmt_float(input):
    output = '{:-4.8f}'.format(input)
    return output

def start_logging():
    global log_start_time
    global order_logger

    log_start_time = datetime.utcnow().date()

    order_logger = logging.getLogger('order_tracker')
    order_logger.setLevel(logging.DEBUG)

    base = 'logs\\'
    order_log_file_name = '%smulti_exchange_orders_%s.log' % (base, log_start_time.isoformat())
    order_log_file_handler = logging.FileHandler(order_log_file_name)
    order_log_file_handler.setLevel(logging.INFO)

    # remove and existing log handlers and replace them with the ones we just created
    for handler in order_logger.handlers[:]:
        order_logger.removeHandler(handler)
    order_logger.addHandler(order_log_file_handler)


def check_logs():
    global log_start_time

    # restart all sockets if they've been up more than half a day
    current_time = datetime.utcnow().date()
    if current_time >= log_start_time + ONE_DAY:
        # starting the loggers will close down the old ones.
        start_logging()


def write_log_entries(log_entries):
    global order_logger
    for entry in log_entries:
            print('log line: ', entry.to_string())
            order_logger.info(entry.to_string())

if __name__ == "__main__":
    exception_count = 0
    while True:
        try:
            start_logging()
            start_time = datetime.utcnow()
            binance_exchange = BinanceExchange()
            kucoin_exchange = KucoinExchange()
            cryptopia_exchange = None#CryptopiaExchange()
            for coin in COIN_LIST:
                binance_exchange.balance_book[coin].free = b_balances[coin]
                kucoin_exchange.balance_book[coin].free = k_balances[coin]
                #cryptopia_exchange.balance_book[coin].free = c_balances[coin]
            print('run arb')
            run_arbitrage(binance_exchange, kucoin_exchange, cryptopia_exchange)
            print('finish arb')
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
            binance_exchange = None
            kucoin_exchange = None
            cryptopia_exchange = None
            time.sleep(60)
            gc.collect()
            time.sleep(60)


