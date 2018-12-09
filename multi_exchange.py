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
from exchange import Exchange, Order, COIN_LIST, PAIR_LIST, CYCLE_LIST, PAIR_COINS
from exchange import BTC, ETH, NEO, LTC, USDT, BNB
from exchange import BTCUSDT, ETHBTC, ETHUSDT, NEOBTC, NEOETH, NEOUSDT, LTCUSDT, LTCBTC, LTCETH

ONE_DAY = timedelta(days=1)
log_start_time = None
order_logger = None
exception_logger = None
transaction_logger = None
low_balance_logger = None

class LogEntry():
    version = '1.2'
    name = 'multi_exchange_arbitrage'
    timestamp = None
    pair = None
    bid_exchange = None
    ask_exchange = None
    expected_return = 0.0
    actual_return = 0.0
    best_bid = 0.0
    best_ask = 0.0
    best_bid_adjusted = 0.0
    best_ask_adjusted = 0.0
    best_bid_size = 0.0
    best_ask_size = 0.0
    actual_bid = 0.0
    actual_ask = 0.0
    actual_bid_size = 0.0
    actual_ask_size = 0.0
    bid_fee = {}
    ask_fee = {}
    total_return = {}
    total_profit = 0.0
    bids = {}
    asks = {}
    start_balances = {}
    delta_balances = {}
    coin1 = None
    coin2 = None
    bid_order = None
    ask_order = None


    def to_string(self):

        if self.bid_order is None:
            bid_status = 'None'
        else:
            bid_status = self.bid_order.status
        if self.ask_order is None:
            ask_status = 'None'
        else:
            ask_status = self.ask_order.status
        return_value = [self.name, self.version, self.timestamp.isoformat(), self.pair,
                        self.bid_exchange.name, self.ask_exchange.name,
                        '{:.8f}'.format(100*(self.expected_return-1)), '{:.8f}'.format(100*(self.actual_return-1)),
                        '{:.8f}'.format(self.total_profit), bid_status, ask_status,
                        {'best_bid': self.best_bid, 'best_ask': self.best_ask,
                         'best_bid_adjusted': self.best_bid_adjusted, 'best_ask_adjusted': self.best_ask_adjusted,
                         'best_bid_size': self.best_bid_size, 'best_ask_size': self.best_ask_size},
                        {'actual_bid': self.actual_bid, 'actual_ask': self.actual_ask,
                         'actual_bid_size': self.actual_bid_size, 'actual_ask_size': self.actual_ask_size,
                         'bid_fee': self.bid_fee, 'ask_fee': self.ask_fee},
                        {'bids': self.bids, 'asks': self.asks,
                         'start_balances': self.start_balances, 'delta_balances': self.delta_balances},
                        self.coin1, self.coin2, self.bid_order, self.ask_order]
        return str(return_value)


    def get_latest_order_data(self):
        bid_updated = False
        ask_updated = False
        if self.bid_order is not None:
            if self.bid_order.status != Order.FILLED:
                self.bid_exchange.update_order(self.bid_order)
            if self.bid_order.status == Order.FILLED:
                self.actual_bid = self.bid_order.price
                self.actual_bid_size = self.bid_order.executed_quantity
                self.bid_fee = self.bid_order.fee
                # updated self.delta_balances
                # self.delta_balances[self.bid_exchange.name][self.coin1] = -self.actual_bid_size
                # self.delta_balances[self.bid_exchange.name][self.coin2] = (self.actual_bid_size * self.actual_bid) * (1 - self.bid_exchange.FEE)
                self.delta_balances[self.bid_exchange.name][self.coin1] = -self.actual_bid_size
                self.delta_balances[self.bid_exchange.name][self.coin2] = self.actual_bid_size * self.actual_bid
                for coin in self.bid_fee:
                    if coin in self.delta_balances[self.bid_exchange.name]:
                        self.delta_balances[self.bid_exchange.name][coin] -= self.bid_fee[coin]
                    else:
                        self.delta_balances[self.bid_exchange.name][coin] = -self.bid_fee[coin]
                bid_updated = True
        if self.ask_order is not None:
            if self.ask_order.status != Order.FILLED:
                self.ask_exchange.update_order(self.ask_order)
            if self.ask_order.status == Order.FILLED:
                self.actual_ask = self.ask_order.price
                self.actual_ask_size = self.ask_order.executed_quantity
                self.ask_fee = self.ask_order.fee
                # updated self.delta_balances
                # self.delta_balances[self.ask_exchange.name][self.coin1] = self.actual_ask_size
                # self.delta_balances[self.ask_exchange.name][self.coin2] = -(self.actual_ask_size * self.actual_ask / (1 - self.ask_exchange.FEE))
                self.delta_balances[self.ask_exchange.name][self.coin1] = self.actual_ask_size
                self.delta_balances[self.ask_exchange.name][self.coin2] = -(self.actual_ask_size * self.actual_ask)
                for coin in self.ask_fee:
                    if coin in self.delta_balances[self.ask_exchange.name]:
                        self.delta_balances[self.ask_exchange.name][coin] -= self.ask_fee[coin]
                    else:
                        self.delta_balances[self.ask_exchange.name][coin] = -self.ask_fee[coin]
                ask_updated = True
        if not bid_updated:
            print('Bid not updated: {}'.format(self))
        elif not ask_updated:
            print('Ask not updated: {}'.format(self))
        else:
            for coin in self.delta_balances['binance']:
                self.delta_balances['all'][coin] = self.delta_balances['binance'][coin]
            for coin in self.delta_balances['kucoin']:
                if coin in self.delta_balances['all']:
                    self.delta_balances['all'][coin] += self.delta_balances['kucoin'][coin]
                else:
                    self.delta_balances['all'][coin] = self.delta_balances['kucoin'][coin]

            # self.delta_balances['all'][self.coin1] = self.delta_balances['binance'][self.coin1] + self.delta_balances['kucoin'][self.coin1]
            # self.delta_balances['all'][self.coin2] = self.delta_balances['binance'][self.coin2] + self.delta_balances['kucoin'][self.coin2]
            self.actual_return = (self.actual_bid * (1 - self.bid_exchange.FEE)) / (self.actual_ask / (1 - self.ask_exchange.FEE))
            #self.actual_return = self.delta_balances['binance'][self.coin1] * price

balances_set = False
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
fees = {BNB: 0.0,
        BTC: 0.0,
        ETH: 0.0,
        LTC: 0.0,
        NEO: 0.0,
        USDT: 0.0}

total_return = {BTC: 0.0,
                ETH: 0.0,
                LTC: 0.0,
                NEO: 0.0,
                USDT: 0.0}

# price = {BTC: 8000.0,
#          ETH: 700.0,
#          LTC: 150.0,
#          NEO: 60.0,
#          USDT: 1.0}

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
    global fees
    global exception_logger
    global low_balance_logger

    pending_entry_list = []

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

            start_balance = {'binance': {coin: binance_exchange.balance_book[coin].free for coin in binance_exchange.balance_book if coin in COIN_LIST or coin == BNB},
                             'kucoin': {coin: kucoin_exchange.balance_book[coin].free for coin in kucoin_exchange.balance_book if coin in COIN_LIST}}
            all_transactions = []
            binance_exchange.refill_bnb(start_balance['binance'], all_transactions)
            # kucoint_start_time = datetime.utcnow()
            # kucoin_exchange.update_raw_order_book()
            # kucoin_end_time = datetime.utcnow()
            # # cryptopia_exchange.update_raw_order_book()
            # binance_exchange.update_raw_order_book()
            # binance_end_time = datetime.utcnow()
            # delta1 = kucoin_end_time - kucoint_start_time
            # delta2 = binance_end_time - kucoin_end_time
            # print('Kucoin time: {} Binance time: {}'.format(delta1, delta2))

            sell_price = {USDT: 1.0}
            for coin in COIN_LIST+[BNB]:
                if coin+USDT in binance_exchange.raw_order_book:
                    sell_price[coin] = binance_exchange.raw_order_book[coin+USDT].bid

            filled_entry_list = []
            # replace the following with the preceding when we start using actual balances
            # Check for simple arbitrage
            for pair in PAIR_LIST:
                balance_low = {'binance': [],
                               'kucoin': []}
                #cryptopia_exchange.update_raw_order_pair(pair)
                binance_exchange.update_raw_order_pair(pair)
                kucoin_exchange.update_raw_order_pair(pair)
                b_bid = binance_exchange.raw_order_book[pair].bid
                b_ask = binance_exchange.raw_order_book[pair].ask
                k_bid = kucoin_exchange.raw_order_book[pair].bid
                k_ask = kucoin_exchange.raw_order_book[pair].ask
                print('Pair: {:<7} B-Bid: {:<8} K-Bid: {:<10} B-Ask: {:<8} K-Ask: {:<10}'.format(pair, b_bid, k_bid, b_ask, k_ask))
                b_bid_fee = b_bid * (1-binance_exchange.FEE)
                b_ask_fee = b_ask / (1-binance_exchange.FEE)
                k_bid_fee = k_bid * (1-kucoin_exchange.FEE)
                k_ask_fee = k_ask / (1-kucoin_exchange.FEE)
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
                        print('Profit: {} sell binance {} is too low'.format(b_bid_fee/k_ask_fee, coin1))
                        continue
                    elif b_bid_fee / k_ask_fee < 1.0001:
                        print('Profit: {} sell binance {} is too low'.format(b_bid_fee/k_ask_fee, coin1))
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
                        balance_low['binance'].append(coin1)
                        low_balance_logger.error('[{}, {:7>}, {:7>}, {:4>}, {:-4.8f}, {:-4.8f}]'.format(datetime.utcnow().isoformat(),
                                                                                                        'binance',
                                                                                                        pair,
                                                                                                        coin1,
                                                                                                        b_free[coin1],
                                                                                                        0.001 * a_balances[coin1]))
                        continue
                    if k_free[coin2] < volume * k_ask / (1 - kucoin_exchange.FEE):
                        print('Kucoin {} balance too small {}'.format(coin2, volume * k_ask / (1 - kucoin_exchange.FEE)))
                        balance_low['kucoin'].append(coin2)
                        low_balance_logger.error('[{}, {:7>}, {:7>}, {:4>}, {:-4.8f}, {:-4.8f}]'.format(datetime.utcnow().isoformat(),
                                                                                                        'kucoin',
                                                                                                        pair,
                                                                                                        coin2,
                                                                                                        k_free[coin2],
                                                                                                        volume * k_ask / (1 - kucoin_exchange.FEE)))
                        continue
                    if volume < binance_exchange.MIN_AMOUNT[pair]:
                        print('Binance {} volume too small {} < {}'.format(coin1,
                                                                           volume,
                                                                           binance_exchange.MIN_AMOUNT[pair]))
                        continue
                    if (volume * b_bid) < binance_exchange.MIN_NOTIONAL[pair]:
                        print('Binance {} return volume under min notional {} < {}'.format(coin2,
                                                                                           (volume * b_bid),
                                                                                           binance_exchange.MIN_NOTIONAL[pair]))
                        continue
                    if volume * k_ask < kucoin_exchange.MIN_AMOUNT[pair]:
                        print('Kucoin {} volume too small {} < {}'.format(coin2,
                                                                          volume*k_ask,
                                                                          kucoin_exchange.MIN_AMOUNT[pair]))
                        continue
                    if b_bid_fee / k_ask_fee < 1.00005:
                        if volume > 0.5 * b_bid_max:
                            print('Binance slippage concern {} {} {}'.format(b_bid_fee/k_ask_fee, volume, b_bid_max))
                            continue
                        if volume > 0.5 * k_ask_max:
                            print('Kucoin slippage concern {} {} {}'.format(b_bid_fee / k_ask_fee, volume, k_ask_max))
                            continue
                    if b_free[coin1] > volume and k_free[coin2] > volume * k_ask:
                        # sell on binance
                        # buy on kucoin
                        # bid_order = binance_exchange.market_convert_coins(coin1, coin2, volume)
                        # ask_order = kucoin_exchange.market_convert_coins(coin2, coin1, volume * b_bid)#, alt_min=volume)
                        bid_order = binance_exchange.market_sell_coins(coin1, coin2, volume)
                        ask_order = kucoin_exchange.market_buy_coins(coin1, coin2, volume / (1 - kucoin_exchange.FEE))

                        # binance_exchange.balance_book[coin1].free -= volume
                        # binance_exchange.balance_book[coin2].free += volume * b_bid
                        # kucoin_exchange.balance_book[coin1].free += volume * (b_bid / k_ask) * (1 - kucoin_exchange.FEE)
                        # kucoin_exchange.balance_book[coin2].free -= volume * b_bid
                        binance_exchange.balance_book[coin1].free -= volume
                        binance_exchange.balance_book[coin2].free += volume * b_bid
                        kucoin_exchange.balance_book[coin1].free += volume
                        kucoin_exchange.balance_book[coin2].free -= (volume / (1 - kucoin_exchange.FEE)) * k_ask
                        direction = 'sell binance'

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
                                                                coin2: (volume * b_bid)},
                                                    'kucoin': {coin1: volume * (b_bid / k_ask) * (1 - kucoin_exchange.FEE),
                                                               coin2: -(volume * b_bid)},
                                                    'all': {coin1: (volume * (b_bid / k_ask) * (1 - kucoin_exchange.FEE))
                                                                   - volume,
                                                            coin2: 0}}
                        log_entry.total_return = {coin1: log_entry.delta_balances['all'][coin1] * sell_price[coin1],
                                                  coin2: log_entry.delta_balances['all'][coin2] * sell_price[coin2]}
                        log_entry.bid_exchange = binance_exchange
                        log_entry.ask_exchange = kucoin_exchange
                        log_entry.bid_order = bid_order
                        log_entry.ask_order = ask_order
                        pending_entry_list.append(log_entry)
                        if log_entry.total_return[coin2] < 0:
                            write_log_entries(log_entries)
                            print('negative return!!!!!')
                            exit(1)
                    else:
                        continue
                    print('profit! {} {}'.format(b_bid_fee / k_ask_fee, direction))
                elif k_bid_fee > b_ask_fee:
                    if k_bid_fee / b_ask_fee < 1.0005 and k_free[coin1] < (b_free[coin1]/2):
                        print('Profit: {} sell kucoin {} is too low'.format(k_bid_fee/b_ask_fee, coin1))
                        continue
                    elif k_bid_fee / b_ask_fee < 1.0001:
                        print('Profit: {} sell kucoin {} is too low'.format(k_bid_fee/b_ask_fee, coin1))
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
                        balance_low['kucoin'].append(coin1)
                        low_balance_logger.error('[{}, {:7>}, {:7>}, {:4>}, {:-4.8f}, {:-4.8f}]'.format(datetime.utcnow().isoformat(),
                                                                                                        'kucoin',
                                                                                                        pair,
                                                                                                        coin1,
                                                                                                        k_free[coin1],
                                                                                                        0.001 * a_balances[coin1]))
                        continue
                    if b_free[coin2] < volume * b_ask / (1 - binance_exchange.FEE):
                        print('Binance {} balance too small {}'.format(coin2, volume * b_ask / (1 - binance_exchange.FEE)))
                        balance_low['binance'].append(coin2)
                        low_balance_logger.error('[{}, {:7>}, {:7>}, {:4>}, {:-4.8f}, {:-4.8f}]'.format(datetime.utcnow().isoformat(),
                                                                                                        'binance',
                                                                                                        pair,
                                                                                                        coin2,
                                                                                                        b_free[coin2],
                                                                                                        volume * b_ask / (1 - binance_exchange.FEE)))
                        continue
                    if volume < kucoin_exchange.MIN_AMOUNT[pair]:
                        print('Kucoin {} volume too small {} < {}'.format(coin1, volume, kucoin_exchange.MIN_AMOUNT[pair]))
                        continue
                    if (volume * k_bid) * (1 - kucoin_exchange.FEE) < binance_exchange.MIN_AMOUNT[pair]:
                        print('Binance {} volume too small {} < {}'.format(coin2,
                                                                           (volume * k_bid) * (1 - kucoin_exchange.FEE),
                                                                           binance_exchange.MIN_AMOUNT[pair]))
                        continue
                    if (volume * (k_bid/b_ask)) * (1 - kucoin_exchange.FEE) < binance_exchange.MIN_NOTIONAL[pair]:
                        print('Binance {} return volume under min notional {} < {}'.format(coin1,
                                                                                           (volume * (k_bid/b_ask)) * (1 - kucoin_exchange.FEE),
                                                                                           binance_exchange.MIN_AMOUNT[pair]))
                        continue
                    if k_bid_fee / b_ask_fee < 1.00005:
                        if volume > 0.5 * k_bid_max:
                            print('Kucoin slippage concern {} {} {}'.format(k_bid_fee/b_ask_fee, volume, k_bid_max))
                            continue
                        if volume > 0.5 * b_ask_max:
                            print('Binance slippage concern {} {} {}'.format(k_bid_fee/b_ask_fee, volume, b_ask_max))
                            continue
                    if b_free[coin2] > volume * b_ask and k_free[coin1] > volume:
                        # buy on binance
                        # sell on kucoin
                        # bid_order = kucoin_exchange.market_convert_coins(coin1, coin2, volume)
                        # ask_order = binance_exchange.market_convert_coins(coin2, coin1, (volume * k_bid) * (1 - kucoin_exchange.FEE))#, alt_min=volume)
                        ask_order = binance_exchange.market_buy_coins(coin1, coin2, volume)
                        bid_order = kucoin_exchange.market_sell_coins(coin1, coin2, volume)

                        # kucoin_exchange.balance_book[coin1].free -= volume
                        # kucoin_exchange.balance_book[coin2].free += (volume * k_bid) * (1 - kucoin_exchange.FEE)
                        # binance_exchange.balance_book[coin1].free += (volume * (k_bid/b_ask)) * (1 - kucoin_exchange.FEE)
                        # binance_exchange.balance_book[coin2].free -= (volume * k_bid) * (1 - kucoin_exchange.FEE)
                        binance_exchange.balance_book[coin1].free += volume
                        binance_exchange.balance_book[coin2].free -= volume * b_ask
                        kucoin_exchange.balance_book[coin1].free -= volume
                        kucoin_exchange.balance_book[coin2].free += (volume * k_bid) * (1 - kucoin_exchange.FEE)
                        direction = 'sell kucoin'

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
                        binance_exchange.balance_book[coin1].free += (volume * (k_bid/b_ask)) * (1 - kucoin_exchange.FEE)
                        binance_exchange.balance_book[coin2].free -= (volume * k_bid) * (1 - kucoin_exchange.FEE)
                        kucoin_exchange.balance_book[coin1].free -= volume
                        kucoin_exchange.balance_book[coin2].free += (volume * k_bid) * (1 - kucoin_exchange.FEE)
                        log_entry.delta_balances = {'binance': {coin1: (volume * (k_bid/b_ask)) * (1 - kucoin_exchange.FEE),
                                                                coin2: -(volume * k_bid) * (1 - kucoin_exchange.FEE)},
                                                    'kucoin': {coin1: -volume,
                                                               coin2: (volume * k_bid) * (1 - kucoin_exchange.FEE)},
                                                    'all': {coin1: (volume * (k_bid/b_ask)) * (1 - kucoin_exchange.FEE)
                                                                   - volume,
                                                            coin2: 0}}
                        log_entry.total_return = {coin1: log_entry.delta_balances['all'][coin1] * sell_price[coin1],
                                                  coin2: log_entry.delta_balances['all'][coin2] * sell_price[coin2]}
                        log_entry.bid_exchange = kucoin_exchange
                        log_entry.ask_exchange = binance_exchange
                        log_entry.bid_order = bid_order
                        log_entry.ask_order = ask_order
                        pending_entry_list.append(log_entry)
                        if log_entry.total_return[coin2] < 0:
                            write_log_entries(log_entries)
                            print('negative return!!!!!')
                            exit(1)
                    else:
                        continue
                    print('profit! {} {}'.format(k_bid_fee/b_ask_fee, direction))

            # see if we need to cancel any orders
            #kucoin_reset_needed = False
            kucoin_pending_coin_list = []
            for entry in pending_entry_list[:]:
                print('Checking on entry: {}'.format(entry.__dict__))
                if entry.bid_order is None or entry.ask_order is None:
                    #bid_order_length = 0
                    #ask_order_length = 0
                    if entry.bid_order is not None:
                        entry.bid_exchange.cancel_order(entry.bid_order)
                        #bid_order_length = entry.bid_order.timestamp_cleared - entry.bid_order.timestamp_placed
                    if entry.ask_order is not None:
                        entry.ask_exchange.cancel_order(entry.ask_order)
                        #ask_order_length = entry.ask_order.timestamp_cleared - entry.ask_order.timestamp_placed
                    print('Order canceled')
                    #print('order lengths: {} {}'.format(bid_order_length, ask_order_length))
                    #if bid_order_length > timedelta(seconds=10) or ask_order_length > timedelta(seconds=10):
                    #    print('reset needed')
                    #    kucoin_reset_needed = True
                    filled_entry_list.append(entry)
                    pending_entry_list.remove(entry)
                    continue

                if entry.bid_order.status is not Order.FILLED:
                    entry.bid_exchange.update_order(entry.bid_order)
                if entry.ask_order.status is not Order.FILLED:
                    entry.ask_exchange.update_order(entry.ask_order)

                if entry.bid_order.status == Order.FILLED and entry.ask_order.status == Order.FILLED:
                    print('Order filled')
                    bid_order_length = entry.bid_order.timestamp_cleared - entry.bid_order.timestamp_placed
                    ask_order_length = entry.ask_order.timestamp_cleared - entry.ask_order.timestamp_placed
                    print('order lengths: {} {}'.format(bid_order_length, ask_order_length))
                    if bid_order_length > timedelta(seconds=10) or ask_order_length > timedelta(seconds=10):
                        print('reset needed')
                        kucoin_reset_needed = True
                    filled_entry_list.append(entry)
                    pending_entry_list.remove(entry)
                    continue

                # if we are running low on the coin for this order, cancel it
                if entry.bid_order.status not in [Order.FILLED, Order.CANCELED]:
                    open_order = entry.bid_order
                    if open_order.reserve_coin in balance_low[open_order.exchange] and \
                            current_time - open_order.timestamp_placed > timedelta(minutes=30):
                        print('{} {} balance too low {}, cancelling order.'.format(open_order.exchange,
                                                                                   open_order.reserve_coin,
                                                                                   open_order.exchange.balance_book[open_order.reserve_coin].free))
                        open_order.exchange.cancel_order(open_order)
                        entry.ask_exchange.cancel_order(entry.ask_order)
                        filled_entry_list.append(entry)
                        pending_entry_list.remove(entry)
                        continue
                    elif entry.bid_order.exchange == 'kucoin':
                        kucoin_pending_coin_list.append(entry.coin1)
                if entry.ask_order.status not in [Order.FILLED, Order.CANCELED]:
                    open_order = entry.ask_order
                    if open_order.reserve_coin in balance_low[open_order.exchange] and \
                            current_time - open_order.timestamp_placed > timedelta(minutes=30):
                        print('Exchange balance too low {}, cancelling order.'.format(open_order.exchange.balance_book[open_order.reserve_coin].free))
                        open_order.exchange.cancel_order(order)
                        entry.bid_exchange.cancel_order(entry.bid_order)
                        filled_entry_list.append(entry)
                        pending_entry_list.remove(entry)
                        continue
                    elif entry.ask_order.exchange == 'kucoin':
                        kucoin_pending_coin_list.append(entry.coin2)

            # process finished orders
            for log_entry in filled_entry_list:
                log_entry.get_latest_order_data()
                bid_filled = False
                ask_filled = False
                if log_entry.bid_order is not None:
                    if log_entry.bid_order.status == Order.FILLED:
                        print('bid order filled')
                        all_transactions.append(log_entry.bid_order)
                        for coin in log_entry.bid_order.fee:
                            fees[coin] += log_entry.bid_order.fee[coin]
                        bid_filled = True
                    elif log_entry.bid_order.status == Order.CANCELED:
                        print('bid order canceled')
                        all_transactions.append(log_entry.bid_order)
                        for coin in log_entry.bid_order.fee:
                            fees[coin] += log_entry.bid_order.fee[coin]
                if log_entry.ask_order is not None:
                    if log_entry.ask_order.status == Order.FILLED:
                        print('ask order filled')
                        all_transactions.append(log_entry.ask_order)
                        for coin in log_entry.ask_order.fee:
                            fees[coin] += log_entry.ask_order.fee[coin]
                        ask_filled = True
                    elif log_entry.ask_order.status == Order.CANCELED:
                        print('ask order canceled')
                        all_transactions.append(log_entry.ask_order)
                        for coin in log_entry.ask_order.fee:
                            fees[coin] += log_entry.ask_order.fee[coin]
                print('bid filled {} ask filled {}'.format(bid_filled, ask_filled))
                if bid_filled and ask_filled:
                    # if log_entry.coin1 != USDT:
                    #     price1 = binance_exchange.raw_order_book[log_entry.coin1 + USDT].bid
                    # else:
                    #     price1 = 1
                    # if log_entry.coin2 != USDT:
                    #     price2 = binance_exchange.raw_order_book[log_entry.coin1 + USDT].bid
                    # else:
                    #     price2 = 1

                    log_entry.total_profit = 0
                    for coin in log_entry.delta_balances['all']:
                        if coin != USDT:
                            log_entry.total_profit += log_entry.delta_balances['all'][coin] * \
                                                      binance_exchange.raw_order_book[coin + USDT].bid
                        else:
                            log_entry.total_profit += log_entry.delta_balances['all'][coin]

                    # log_entry.total_profit = log_entry.delta_balances['all'][log_entry.coin1] * price1 + \
                    #                          log_entry.delta_balances['all'][log_entry.coin2] * price2

            write_log_entries(filled_entry_list)
            update_transaction_log(all_transactions)
            print('******************************************')
            # print('Coin  Bin Balance Kuc Balance Total       Return      Return Val  Fees')
            print('{:<4} {:<13} {:<13} {:<13} {:<13} {:<13} {:<13} {:<13}'.format('Coin', 'Bin Balance', 'Kuc Free',
                                                                                  'Kuc Locked', 'Total', 'Return',
                                                                                  'Return Val', 'Fees'))
            binance_exchange.query_coin_balances()
            #if kucoin_reset_needed:
            #    print('Resetting Kucoin connection')
            #    kucoin_exchange.reset_connection()
            kucoin_exchange.query_coin_balances()
            cumulative_return = 0.0
            cumulative_fees = 0.0
            for coin in sorted(COIN_LIST+[BNB]):
                if coin not in a_balances:
                    continue
                if coin in binance_exchange.balance_book:
                    binance_balance = binance_exchange.balance_book[coin].free + binance_exchange.balance_book[coin].locked
                else:
                    binance_balance = 0
                if coin in kucoin_exchange.balance_book:
                    kucoin_free = kucoin_exchange.balance_book[coin].free
                    if coin in kucoin_pending_coin_list:
                        kucoin_locked = kucoin_exchange.balance_book[coin].locked
                    else:
                        kucoin_locked = 0
                    kucoin_balance = kucoin_free + kucoin_locked
                else:
                    kucoin_free = 0
                    kucoin_locked = 0
                    kucoin_balance = 0

                total_return[coin] = (binance_balance - b_balances[coin]) + \
                                     (kucoin_balance - k_balances[coin])
                print('{:<4} {:>13} {:>13} {:>13} {:>13} {:>13} {:>13} {:>13}'.format(coin,
                                                                                      fmt_float(binance_balance),
                                                                                      fmt_float(kucoin_free),
                                                                                      fmt_float(kucoin_locked),
                                                                                      fmt_float(binance_balance + kucoin_balance),
                                                                                      fmt_float(total_return[coin]),
                                                                                      fmt_float(total_return[coin] * sell_price[coin]),
                                                                                      fmt_float(fees[coin])))
                cumulative_return += total_return[coin] * sell_price[coin]
                cumulative_fees += fees[coin] * sell_price[coin]
            current_time = datetime.utcnow()
            delta = current_time - START_TIME
            total_btc_value = 0.0
            for coin in a_balances:
                if coin == BTC:
                    total_btc_value += a_balances[coin]
                elif coin == USDT:
                    total_btc_value += a_balances[coin] / binance_exchange.raw_order_book[BTCUSDT].ask
                else:
                    total_btc_value += a_balances[coin] * binance_exchange.raw_order_book[coin+BTC].bid

            print('Running time: {:<44}                 {:>13} {:>13} {:>13}'.format(str(delta),
                                                                                     fmt_float(cumulative_return),
                                                                                     fmt_float(cumulative_fees),
                                                                                     fmt_float(total_btc_value)))
            print('******************************************')

            # filled_transactions = []
            # filled_orders_list = []
            # for order in pending_order_list[:]:
            #     print('Checking on bid order: {} {} {} {} {} {}'.format(order.bid_order.id,
            #                                                         datetime.isoformat(order.bid_order.timestamp_placed),
            #                                                         order.bid_order.pair, order.bid_order.reserve_coin, order.bid_order.quantity,
            #                                                         order.bid_order.price))
            #     print('Checking on ask order: {} {} {} {} {} {}'.format(order.ask_order.id,
            #                                                         datetime.isoformat(order.ask_order.timestamp_placed),
            #                                                         order.ask_order.pair, order.ask_order.reserve_coin, order.ask_order.quantity,
            #                                                         order.ask_order.price))
            #     # see if the order has already been filled
            #     # if order.exchange == 'binance':
            #     #     current_exchange = kucoin_exchange
            #     # elif order.exchange == 'kucoin':
            #     #     current_exchange = kucoin_exchange
            #     order.get_latest_order_data()
            #     if order.bid_order.status == 'FILLED' and order.ask_order.status == 'FILLED':
            #         print('Order filled')
            #         filled_transactions.append(order.bid_order)
            #         filled_transactions.append(order.ask_order)
            #         filled_orders_list.append(order)
            #         pending_order_list.remove(order)
            #         continue
            #
            #     # if we are running low on the coin for this order, cancel it
            #     if order.bid_order.status != 'FILLED':
            #         open_order = order.bid_order
            #         if open_order.reserve_coin in balance_low[open_order.exchange] and \
            #                 current_time - open_order.timestamp_placed > timedelta(minutes=30):
            #             print('Exchange balance too low {}, cancelling order.'.format(open_order.exchange.balance_book[open_order.reserve_coin].free))
            #             open_order.exchange.cancel_order(order)
            #             filled_transactions.append(order.bid_order)
            #             filled_transactions.append(order.ask_order)
            #             filled_orders_list.append(order)
            #             pending_order_list.remove(order)
            #             continue
            #     if order.ask_order.status != 'FILLED':
            #         open_order = order.ask_order
            #         if open_order.reserve_coin in balance_low[open_order.exchange] and \
            #                 current_time - open_order.timestamp_placed > timedelta(minutes=30):
            #             print('Exchange balance too low {}, cancelling order.'.format(open_order.exchange.balance_book[open_order.reserve_coin].free))
            #             open_order.exchange.cancel_order(order)
            #             filled_transactions.append(order.bid_order)
            #             filled_transactions.append(order.ask_order)
            #             filled_orders_list.append(order)
            #             pending_order_list.remove(order)
            #             continue
            #     # if current_exchange.balance_book[order.reserve_coin].free < 0.1 * a_balances[order.reserve_coin] and \
            #     #         current_time - order.timestamp_placed > timedelta(minutes=30):
            #     #     print('Exchange balance too low {}, cancelling order.'.format(current_exchange.balance_book[order.reserve_coin].free))
            #     #     current_exchange.cancel_order(order)
            # write_log_entries(filled_orders_list)
            # update_transaction_log(filled_transactions)

            # if len(log_entries) > 0:
            #     print('found an order, exiting')
            #     exit(0)
            # if len(all_transactions) > 0:
            #     print('found a transaction, exiting')
            #     exit(0)

            print('******************************************')
            check_logs()
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


        except Exception as e:
            print('Exception: ', str(e))
            traceback.print_exc()

            exception_logger.error('Time: ' + datetime.utcnow().isoformat())
            exception_logger.error('Exception in run_arbitrage: ', str(e))
            exception_logger.error(traceback.format_exc())


def fmt_float(input):
    output = '{:-4.8f}'.format(input)
    return output


def start_logging():
    global log_start_time
    global order_logger
    global exception_logger
    global transaction_logger
    global low_balance_logger

    log_start_time = datetime.utcnow().date()

    order_logger = logging.getLogger('order_tracker')
    order_logger.setLevel(logging.DEBUG)
    exception_logger = logging.getLogger('exception_tracker')
    exception_logger.setLevel(logging.DEBUG)
    transaction_logger = logging.getLogger('transaction_tracker')
    transaction_logger.setLevel(logging.DEBUG)
    low_balance_logger = logging.getLogger('low_balance_tracker')
    low_balance_logger.setLevel(logging.DEBUG)

    base = 'logs\\'
    order_log_file_name = '%smulti_exchange_orders_%s.log' % (base, log_start_time.isoformat())
    exception_log_file_name = '%smulti_exchange_exceptions_%s.log' % (base, log_start_time.isoformat())
    transaction_log_file_name = '%smulti_exchange_transactions_%s.log' % (base, log_start_time.isoformat())
    low_balance_log_file_name = '%smulti_exchange_low_balance_%s.log' % (base, log_start_time.isoformat())

    order_log_file_handler = logging.FileHandler(order_log_file_name)
    order_log_file_handler.setLevel(logging.INFO)
    exception_log_file_handler = logging.FileHandler(exception_log_file_name)
    exception_log_file_handler.setLevel(logging.INFO)
    transaction_log_file_handler = logging.FileHandler(transaction_log_file_name)
    transaction_log_file_handler.setLevel(logging.INFO)
    low_balance_log_file_handler = logging.FileHandler(low_balance_log_file_name)
    low_balance_log_file_handler.setLevel(logging.INFO)

    # remove and existing log handlers and replace them with the ones we just created
    for handler in order_logger.handlers[:]:
        order_logger.removeHandler(handler)
    order_logger.addHandler(order_log_file_handler)
    for handler in exception_logger.handlers[:]:
        exception_logger.removeHandler(handler)
    exception_logger.addHandler(exception_log_file_handler)
    for handler in transaction_logger.handlers[:]:
        transaction_logger.removeHandler(handler)
    transaction_logger.addHandler(transaction_log_file_handler)
    for handler in low_balance_logger.handlers[:]:
        low_balance_logger.removeHandler(handler)
    low_balance_logger.addHandler(low_balance_log_file_handler)


def check_logs():
    global log_start_time

    # restart all sockets if they've been up more than half a day
    current_time = datetime.utcnow().date()
    if current_time >= log_start_time + ONE_DAY:
        # starting the loggers will close down the old ones.
        start_logging()


def write_log_entries(log_entries):
    global order_logger
    # print('order logger: {}'.format(order_logger))
    for entry in log_entries:
        print('log line: ', entry.to_string())
        order_logger.info(entry.to_string())


def update_transaction_log(transaction_list):
    global exception_logger
    #global transaction_logger
    transaction_logger2 = logging.getLogger('transaction_tracker')
    transaction_logger2.setLevel(logging.DEBUG)
    # print('transaction logger: {}'.format(transaction_logger))
    #transaction_logger2.error('start of update')
    #logging.getLogger('transaction_tracker').error('blah')
    for transaction in transaction_list:
        if transaction is not None and transaction.status is not 'None':
            try:
                if len(transaction.sub_orders) == 0:
                    sub_order = Order()
                    sub_order.price = transaction.price
                    #sub_order.quantity = max(transaction.quantity, 0.0000000001)
                    sub_order.fee = transaction.fee
                    sub_order.id = transaction.id
                    transaction.sub_orders = [sub_order]

                for sub_transaction in transaction.sub_orders:
                    commission = ''
                    for coin, fee in sub_transaction.fee.items():
                        commission += '{}:{:.8f}_'.format(coin, fee)
                    commission = commission[:-1]
                    # commission = '%.8f' % (float(sub_transaction['commission'])*float(sub_transaction['price']))
                    log_list = ['multi_exchange_transactions', 'v1.0',
                                datetime.utcfromtimestamp(transaction.timestamp / 1000.0).isoformat(),
                                transaction.exchange, transaction.pair,
                                float(sub_transaction.price), float(sub_transaction.quantity),
                                float(transaction.executed_quantity),
                                (float(transaction.executed_quantity) / float(sub_transaction.quantity)) if sub_transaction.quantity > 0 else 0,
                                transaction.status, transaction.direction, commission, transaction.memo,
                                transaction.id, transaction.alt_id]
                    log_string = ','.join(str(x) for x in log_list)
                    print('log line: ', log_string)
                    transaction_logger2.info(log_string)
                    # print('transaction written to log')
            except Exception as e:
                exception_logger.error('Time: ' + datetime.utcnow().isoformat())
                exception_logger.error('Exception logging transaction: ', str(transaction))
                exception_logger.error(traceback.format_exc())
                time.sleep(3)


if __name__ == "__main__":
    exception_count = 0
    start_time = datetime.utcnow()
    while True:
        try:
            start_logging()
            start_time = datetime.utcnow()
            binance_exchange = BinanceExchange()
            kucoin_exchange = KucoinExchange()
            cryptopia_exchange = None#CryptopiaExchange()
            binance_exchange.cancel_all_orders()
            kucoin_exchange.cancel_all_orders()
            binance_exchange.query_coin_balances()
            kucoin_exchange.query_coin_balances()

            # set the coin balances the first time we start the program
            if not balances_set:
                for coin in COIN_LIST:
                    b_balances[coin] = binance_exchange.balance_book[coin].free + binance_exchange.balance_book[coin].locked
                    k_balances[coin] = kucoin_exchange.balance_book[coin].free + kucoin_exchange.balance_book[coin].locked
                    a_balances[coin] = b_balances[coin] + k_balances[coin]
                b_balances[BNB] = binance_exchange.balance_book[BNB].free + binance_exchange.balance_book[BNB].locked
                k_balances[BNB] = 0
                a_balances[BNB] = b_balances[BNB]
                balances_set = True

            binance_exchange.update_raw_order_book()
            kucoin_exchange.update_raw_order_book()
            # for coin in COIN_LIST:
            #     # binance_exchange.balance_book[coin].free = b_balances[coin]
            #     # kucoin_exchange.balance_book[coin].free = k_balances[coin]
            #     #cryptopia_exchange.balance_book[coin].free = c_balances[coin]
            #     b_balances[coin] = binance_exchange.balance_book[coin].free
            #     k_balances[coin] = kucoin_exchange.balance_book[coin].free
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


