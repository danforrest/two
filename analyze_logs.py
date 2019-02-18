from datetime import datetime
from dateutil import parser
import traceback
import glob
import json
from pattern_checker import PatternAction

LOG_DIR = 'logs/'
LOG_FILE_GLOB = 'bin_trend_trades_*.log'
if __name__ == "__main__":
    # find all log files
    log_files = glob.glob(LOG_DIR + LOG_FILE_GLOB)
    log_data = {}
    fee_payments = {}

    test_phase_end = parser.parse("2019-02-01T00:00:00")

    print(log_files)
    # read each log file
    for f in log_files:
        print('Reading file: {}'.format(f))
        with open(f, 'r') as log_file:
            # add log contents to data
            log_entries = log_file.readlines()
            entry_count = 0
            for entry in log_entries:
                log_json = json.loads(entry)
                if log_json.get('memo', None) == 'FEE_PAYMENT':
                    fee_date = datetime.fromtimestamp(log_json['transactTime']/1000)
                    fee_payments[fee_date] = log_json
                    print('Fee payment at: {}'.format(fee_date.isoformat()))
                elif not log_json.get('price_in'):
                    print('invalid price in: {}'.format(log_json.get('price_in')))
                elif log_json.get('time_in') == '2018-12-13T05:00:03.888791':
                    print('Anomalous entry: {}'.format(log_json))
                elif 'time_in' in log_json:
                    log_data[parser.parse(log_json['time_in'])] = log_json
                    entry_count += 1
                else:
                    print('Invalid record: {}'.format(log_json))
            print('Added {} records'.format(entry_count))

    test_entries = 0
    timestamp_list = [t for t in sorted(log_data.keys()) if t > test_phase_end]
    fee_list = [f for f in sorted(fee_payments.keys())]

    print('Total records found: {}'.format(len(log_data)))
    print('Total fee payments:  {}'.format(len(fee_payments)))
    print('test entries: {}'.format(test_entries))

    # analyze data
    total_profit = 0
    win_count = 0
    loss_count = 0
    win_r = 0
    loss_r = 0
    win_r_count = 0
    loss_r_count = 0
    profit_list = []
    total_slippage_in = 0
    total_slippage_out = 0
    total_fee = 0
    for timestamp in timestamp_list:
        trade = log_data[timestamp]
        trade_profit = trade.get('profit', 0)
        profit_list.append(trade_profit)
        total_profit += trade_profit
        if trade_profit > 0:
            win_count += 1
        elif trade_profit < 0:
            loss_count += 1
        r = trade.get('R', 0)
        if r > 0:
            win_r += r
            win_r_count += 1
        if r < 0:
            loss_r += r
            loss_r_count += 1
        # calculate slippage
        price_in = trade['price_in']
        price_in_target = trade['price_in_target']
        price_out = trade['price_out']
        price_out_target = trade['price_out_target']
        size_in = trade['position_size']
        size_out = float(trade['exit_order']['executedQty'])
        slippage_in = 0
        slippage_out = 0
        if trade['direction'] == PatternAction.GO_LONG:
            slippage_in = (price_in - price_in_target) * size_in
            slippage_out = (price_out_target - price_out) * size_out
            print('Bull slippage: {} {: .4f} {: .4f} {: .4f} {: .4f}'.format(trade['time_out'], slippage_in, slippage_out, trade_profit, r))
        elif trade['direction'] == PatternAction.GO_SHORT:
            slippage_in = (price_in_target - price_in) * size_in
            slippage_out = (price_out - price_out_target) * size_out
            print('Bear slippage: {} {: .4f} {: .4f} {: .4f} {: .4f}'.format(trade['time_out'], slippage_in, slippage_out, trade_profit, r))
        total_slippage_in += slippage_in
        total_slippage_out += slippage_out

        # fees
        current_fee = fee_payments[fee_list[0]]
        for fee_timestamp in fee_list:
            if fee_timestamp > timestamp:
                break
            current_fee = fee_payments[fee_timestamp]

        if 'fee_price' not in current_fee:
            cumulative_fee = 0
            cumulative_size = 0
            for fill in current_fee['fills']:
                cumulative_fee += float(fill['price']) * float(fill['qty'])
                cumulative_size += float(fill['qty'])
            fee_price = cumulative_fee / cumulative_size
            current_fee['fee_price'] = fee_price
        fee = trade['fee']['BNB'] * current_fee['fee_price']
        total_fee += fee


    profit_list.sort()
    print('total profit: {}'.format(total_profit))
    print('wins: {} losses: {}'.format(win_count, loss_count))
    print('win r: {} loss r: {}'.format(win_r/win_r_count, loss_r/loss_r_count))
    if win_count != win_r_count or loss_count != loss_r_count:
        print('They dont match: wins {}/{} Losses: {}/{}'.format(win_count, win_r_count, loss_count, loss_r_count))
    print('profits: {}'.format(profit_list))
    print('slippage: {} {}'.format(total_slippage_in, total_slippage_out))
    print('fees: {}'.format(total_fee))
