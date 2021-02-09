from datetime import datetime
from datetime import timedelta
import sys, pickle, time
import os.path
from statistics import median

class BackTester(object):

    def __init__(self):
        self.init_funds = 10000
        self.interleave = 5000
        self.sim_init_funds = 10000
        self.sim_interleave = 10000
        self.maximum_lot_size = 50
        self.spread = 4.0 * 10.0
        
    def _load_obj(self, obj_name):
        with open(obj_name, 'rb') as f:
            return pickle.load(f)
            
    def _close_position(self, cur_trading_records, desired_direction, cur_close, cur_datetime):
        decision = 1
        if len(cur_trading_records) > 0 and cur_trading_records[-1]['status'] == 'open':
            direction = cur_trading_records[-1]['direction']
            
            if desired_direction == direction:
                decision = 0
            else:
                cur_trading_records[-1]['profit'] = direction * 1000.0 * (cur_close - cur_trading_records[-1]['execute_price']) - self.spread
                cur_trading_records[-1]['status'] = 'close'
                cur_trading_records[-1]['close_price'] = cur_close
                cur_trading_records[-1]['close_time'] = cur_datetime
                cur_trading_records[-1]['close_reason'] = 'FP'
                decision = 1
        return decision
        
    def _exe_cur_strategy(self, strategy_dict, data_dict, day_search_dict):
        
        stop_mode = str(strategy_dict['STOP'].split('#')[0])
        stopLoss = int(strategy_dict['STOP'].split('#')[1])
        trailing_mode = strategy_dict['trailing_mode']
        target_profit_mode = int(strategy_dict['target_profit'].split('#')[0])
        target_profit = int(strategy_dict['target_profit'].split('#')[1])
        hour_S1 = int(strategy_dict['hour_S1'])
        hour_S3 = int(strategy_dict['hour_S3'])
        direction_flip_1 = int(strategy_dict['direction_flip_1'])
        direction_flip_2 = int(strategy_dict['direction_flip_2'])
        flip_operation_1 = int(strategy_dict['flip_operation_1'])
        flip_operation_2 = int(strategy_dict['flip_operation_2'])
        decision_type_1 = int(strategy_dict['decision_type_1'])
        decision_type_2 = int(strategy_dict['decision_type_2'])
        target_close_FMT = int(strategy_dict['target_close_FMT'])
        flip_1 = int(strategy_dict['flip_1'])
        flip_2 = int(strategy_dict['flip_2'])
        
        trading_records = {}
        cur_funds = self.init_funds
        
        for day in sorted(data_dict):
            trading_records[day] = []
            
            cur_day_data = data_dict[day]['CONTENT']
            
            if day in day_search_dict and day_search_dict[day] in day_search_dict and day_search_dict[day] in data_dict and day_search_dict[day_search_dict[day]] in data_dict:
                prev_day = day_search_dict[day]
                prev_day_2 = day_search_dict[prev_day]
                
                Open_daily_prev_2 = data_dict[prev_day_2]['OHLC_D']['Open_D']
                High_daily_prev_2 = data_dict[prev_day_2]['OHLC_D']['High_D']
                Low_daily_prev_2 = data_dict[prev_day_2]['OHLC_D']['Low_D']
                Close_daily_prev_2 = data_dict[prev_day_2]['OHLC_D']['Close_D']
                Open_time_prev_2 = data_dict[prev_day_2]['OHLC_D']['Open_Time_D']
                Close_time_prev_2 = data_dict[prev_day_2]['OHLC_D']['Close_Time_D']
                High_time_prev_2 = data_dict[prev_day_2]['OHLC_D']['High_Time_D']
                Low_time_prev_2 = data_dict[prev_day_2]['OHLC_D']['Low_Time_D']
                
                Open_daily_prev = data_dict[prev_day]['OHLC_D']['Open_D']
                High_daily_prev = data_dict[prev_day]['OHLC_D']['High_D']
                Low_daily_prev = data_dict[prev_day]['OHLC_D']['Low_D']
                Close_daily_prev = data_dict[prev_day]['OHLC_D']['Close_D']
                Open_time_prev = data_dict[prev_day]['OHLC_D']['Open_Time_D']
                Close_time_prev = data_dict[prev_day]['OHLC_D']['Close_Time_D']
                High_time_prev = data_dict[prev_day]['OHLC_D']['High_Time_D']
                Low_time_prev = data_dict[prev_day]['OHLC_D']['Low_Time_D']
                
                Open_daily_cur = data_dict[day]['OHLC_D']['Open_D']
                Close_daily_cur = data_dict[day]['OHLC_D']['Close_D']
                Open_time_cur = data_dict[day]['OHLC_D']['Open_Time_D']
                Close_time_cur = data_dict[day]['OHLC_D']['Close_Time_D']
                
                prev_body_2 = Close_daily_prev_2 - Open_daily_prev_2
                prev_body = Close_daily_prev - Open_daily_prev
                
                prev_range = High_daily_prev - Low_daily_prev
                prev_close_pct = float(Close_daily_prev - Low_daily_prev) * 100.0 / float(prev_range)
                
                prev_close_FMT = int(prev_close_pct / 20.0)
                if len(cur_day_data) == 24 and len(data_dict[prev_day]['CONTENT']) == 24 and len(data_dict[prev_day_2]['CONTENT']) == 24:
                    
                    mul = float(cur_funds) / float(self.interleave)
                    if mul > self.maximum_lot_size:
                        mul = self.maximum_lot_size
                    elif mul < 0.01:
                        mul = 0.01
                    
                    index = 0
                    for line_dict in cur_day_data:
                        Date = line_dict['Date']
                        Time = line_dict['Time']
                        Open = line_dict['Open']
                        High = line_dict['High']
                        Low = line_dict['Low']
                        Close = line_dict['Close']
                        cur_hour = line_dict['Hour']
                        cur_minute = line_dict['Minute']
                        Raw_index_cur = line_dict['Raw_index']
                        HL1_cur = line_dict['HL1']
                        HL2_cur = line_dict['HL2']
                        HL3_cur = line_dict['HL3']
                        
                        cur_hour_format = hour_mapper_seq[int(cur_hour)]
                        # strategy begin
                        
                        if Close > Open_daily_cur:
                            cur_isAboveOpen = 1
                        else:
                            cur_isAboveOpen = -1
                        
                        if Close > High_daily_prev:
                            cur_isAbovePH = 1
                        else:
                            cur_isAbovePH = -1
                        
                        if Close < Low_daily_prev:
                            cur_isBelowPL = 1
                        else:
                            cur_isBelowPL = -1
                        
                        the_decision_1 = -10
                        if decision_type_1 == 1:
                            the_decision_1 = cur_isAboveOpen
                        elif decision_type_1 == 2:
                            the_decision_1 = cur_isAbovePH
                        elif decision_type_1 == 3:
                            the_decision_1 = cur_isBelowPL
                            
                        the_decision_2 = -10
                        if decision_type_2 == 1:
                            the_decision_2 = cur_isAboveOpen
                        elif decision_type_2 == 2:
                            the_decision_2 = cur_isAbovePH
                        elif decision_type_2 == 3:
                            the_decision_2 = cur_isBelowPL
                            
                        cur_hour_pos = float(Close - Low_daily_prev) * 100.0 / float(prev_range)
                        
                        cur_hour_FMT = int(cur_hour_pos / 20.0)
                        if cur_hour_FMT < 0:
                            cur_hour_FMT = -1
                        elif cur_hour_FMT > 5:
                            cur_hour_FMT = 5
                        
                        if len(trading_records[day]) == 0:
                            if prev_close_FMT == target_close_FMT:
                                if the_decision_1 == flip_1 and cur_hour_format == hour_S1:
                                    if flip_operation_1 == 1:
                                        order_type = 'REAL'
                                        status = 'open'
                                    else:
                                        order_type = 'PLACE_HOLDER'
                                        status = 'C'
                                        
                                    if stop_mode == 'F':
                                        stop_price = Close - direction_flip_1 * (float(stopLoss) / 1000.0)
                                    else:
                                        if direction_flip_1 == 1:
                                            stop_price = Low_daily_prev - (float(stopLoss) / 1000.0) - self.spread
                                        else:
                                            stop_price = High_daily_prev + (float(stopLoss) / 1000.0) + self.spread
                                    
                                    open_info = {
                                        'direction': 1 * direction_flip_1,
                                        'execute_price': Close,
                                        'reason': 'E1',
                                        'order_type': order_type,
                                        'status': status,
                                        'profit': 0,
                                        'mul': mul,
                                        'open_time': Date + ' ' + Time,
                                        'stop_price': stop_price,
                                        'max_gain_val': -9999.999,
                                        'pnl_list': []
                                    }
                                    trading_records[day].append(open_info)
                        else:
                            if trading_records[day][-1]['reason'] == 'E1' and the_decision_2 == flip_2 and cur_hour_format == hour_S3:
                                can_trade = 0
                                if trading_records[day][-1]['status'] == 'open':
                                    if trading_records[day][-1]['order_type'] == 'REAL':
                                        if trading_records[day][-1]['direction'] != direction_flip_2:
                                            trading_records[day][-1]['profit'] = trading_records[day][-1]['direction'] * 1000.0 * (Close - trading_records[day][-1]['execute_price']) - self.spread
                                            trading_records[day][-1]['status'] = 'close'
                                            trading_records[day][-1]['close_price'] = Close
                                            trading_records[day][-1]['close_time'] = Date + ' ' + Time
                                            trading_records[day][-1]['close_reason'] = 'FP1'
                                            can_trade = 1
                                    else:
                                        trading_records[day][-1]['profit'] = 0
                                        trading_records[day][-1]['status'] = 'close'
                                        trading_records[day][-1]['close_price'] = Close
                                        trading_records[day][-1]['close_time'] = Date + ' ' + Time
                                        trading_records[day][-1]['close_reason'] = 'FP2'
                                        can_trade = 1
                                        
                                if stop_mode == 'F':
                                    stop_price = Close - direction_flip_2 * (float(stopLoss) / 1000.0)
                                else:
                                    if direction_flip_2 == 1:
                                        stop_price = Low_daily_prev - (float(stopLoss) / 1000.0) - self.spread
                                    else:
                                        stop_price = High_daily_prev + (float(stopLoss) / 1000.0) + self.spread
                                        
                                if flip_operation_2 == 1 and can_trade == 1:
                                    open_info = {
                                        'direction': 1 * direction_flip_2,
                                        'execute_price': Close,
                                        'reason': 'E3',
                                        'order_type': 'REAL',
                                        'status': 'open',
                                        'profit': 0,
                                        'mul': mul,
                                        'open_time': Date + ' ' + Time,
                                        'stop_price': stop_price,
                                        'max_gain_val': -9999.999,
                                        'pnl_list': []
                                    }
                                    trading_records[day].append(open_info)
                                    
                        # stop price calculation:
                        if len(trading_records[day]) > 0:
                                        
                            if trading_records[day][-1]['status'] == 'open' and trading_records[day][-1]['order_type'] == 'REAL':
                                if trading_records[day][-1]['direction'] == 1:
                                    if Low < trading_records[day][-1]['stop_price']:
                                        trading_records[day][-1]['profit'] = 1000.0 * (trading_records[day][-1]['stop_price'] - trading_records[day][-1]['execute_price']) - self.spread
                                        trading_records[day][-1]['status'] = 'close'
                                        trading_records[day][-1]['close_price'] = trading_records[day][-1]['stop_price']
                                        trading_records[day][-1]['close_time'] = Date + '·' + Time
                                        trading_records[day][-1]['close_reason'] = 'SL'
                                        
                                elif trading_records[day][-1]['direction'] == -1:
                                    if High > trading_records[day][-1]['stop_price']:
                                        trading_records[day][-1]['profit'] = 1000.0 * (trading_records[day][-1]['execute_price'] - trading_records[day][-1]['stop_price']) - self.spread
                                        trading_records[day][-1]['status'] = 'close'
                                        trading_records[day][-1]['close_price'] = trading_records[day][-1]['stop_price']
                                        trading_records[day][-1]['close_time'] = Date + '·' + Time
                                        trading_records[day][-1]['close_reason'] = 'SL'
                                        
                            if trailing_mode == 1 and trading_records[day][-1]['status'] == 'open' and trading_records[day][-1]['order_type'] == 'REAL':
                                if trading_records[day][-1]['direction'] == 1:
                                    if High - trading_records[day][-1]['stop_price'] > float(stopLoss) / 1000.0:
                                        trading_records[day][-1]['stop_price'] = High - float(stopLoss) / 1000.0
                                        
                                elif trading_records[day][-1]['direction'] == -1:
                                    if trading_records[day][-1]['stop_price'] - Low > float(stopLoss) / 1000.0:
                                        trading_records[day][-1]['stop_price'] = Low + float(stopLoss) / 1000.0
                                        
                            if target_profit_mode == 1 and trading_records[day][-1]['status'] == 'open' and trading_records[day][-1]['order_type'] == 'REAL':
                                if trading_records[day][-1]['direction'] == 1:
                                    if High - trading_records[day][-1]['execute_price'] > float(target_profit) / 1000.0:
                                        trading_records[day][-1]['profit'] = target_profit - self.spread
                                        trading_records[day][-1]['status'] = 'close'
                                        trading_records[day][-1]['close_price'] = trading_records[day][-1]['execute_price'] + (float(target_profit) / 1000.0)
                                        trading_records[day][-1]['close_time'] = Date + '·' + Time
                                        trading_records[day][-1]['close_reason'] = 'TP'
                                        
                                elif trading_records[day][-1]['direction'] == -1:
                                    if trading_records[day][-1]['execute_price'] - Low > float(target_profit) / 1000.0:
                                        trading_records[day][-1]['profit'] = target_profit - self.spread
                                        trading_records[day][-1]['status'] = 'close'
                                        trading_records[day][-1]['close_price'] = trading_records[day][-1]['execute_price'] - (float(target_profit) / 1000.0)
                                        trading_records[day][-1]['close_time'] = Date + '·' + Time
                                        trading_records[day][-1]['close_reason'] = 'TP'
                        #print(day, cur_hour, cur_minute, Time, Date, Open, High, Low, Close)
                        
                        index += 1
                        
                        #if cur_funds < 0:
                        #    print('you DEAD!!')
                        #    sys.exit()
                        
                        # print(day, trading_records[day])
                    
                    if len(trading_records[day]) > 0 and trading_records[day][-1]['status'] == 'open' and trading_records[day][-1]['order_type'] == 'REAL':
                        direction = trading_records[day][-1]['direction']
                        trading_records[day][-1]['profit'] = direction * 1000.0 * (Close_daily_cur - trading_records[day][-1]['execute_price']) - self.spread
                        trading_records[day][-1]['status'] = 'close'
                        trading_records[day][-1]['close_price'] = Close_daily_cur
                        trading_records[day][-1]['close_time'] = Date + '·' + Time
                        trading_records[day][-1]['close_reason'] = 'EOD'
                    
                    for item in trading_records[day]:
                        if item['status'] == 'open':
                            print('ERROR_STATUS!!!')
                            sys.exit()
                        
                    for info in trading_records[day]:
                        cur_funds += info['profit'] * info['mul']
        
        return trading_records
        
    def process(self):
        
        data_dict = self._load_obj(pk_file)
        day_search_dict = self._load_obj(day_search_file)
        filted_data_dict = {}
        
        if data_mode == 'YEAR':
            for day in data_dict:
                year = day.split('-')[0]
                month = day.split('-')[1]
                if int(year) == target_year and int(month) == 1:
                    filted_data_dict[day] = data_dict[day]
        elif data_mode == 'WHOLE':
            filted_data_dict = data_dict
        elif data_mode == 'MONTH':
            for day in data_dict:
                year = day.split('-')[0]
                if int(year) == target_year:
                    filted_data_dict[day] = data_dict[day]
        
        print('filted_data_dict', len(filted_data_dict), len(data_dict))
        max_gain = -1
        index = 0
        output_file = output_folder + '/' + sim_card_file.split('/')[-1].split('.')[0] + '_result.txt'
        output_file_seq = output_folder_seq + '/' + sim_card_file.split('/')[-1].split('.')[0] + '_seq.txt'
        with open(sim_card_file, 'r') as f, open(output_file, 'a') as writter, open(output_file_seq, 'a') as writter_seq:
            for line in f:
                
                line_list = line.strip('\n').split(',')
                
                if index == 0:
                    layout_dict = {}
                    ii = 0
                    for feature_name in line_list:
                        layout_dict[feature_name] = ii
                        ii += 1
                else:
                    start_time = time.time()
                    strategy_dict = {}
                    for feature_name in layout_dict:
                        strategy_dict[feature_name] = line_list[layout_dict[feature_name]]
                        
                    trading_records = self._exe_cur_strategy(strategy_dict, filted_data_dict, day_search_dict)
                    
                    stat_funds = self.init_funds
                    month_dict = {}
                    trade_count = 0
                    win_count = 0
                    loss_count = 0
                    long_count = 0
                    short_count = 0
                    max_funds = -999
                    min_funds = 999999999
                    profit_list = []
                    funds_curve_list = []
                    
                    for day in sorted(filted_data_dict):
                        month = day.split('-')[1]
                        if month not in month_dict:
                            month_dict[month] = []
                            
                        if day in trading_records:
                            for cur_record in trading_records[day]:
                                if cur_record['order_type'] != 'PLACE_HOLDER':
                                    month_dict[month].append(cur_record)
                                    
                                    direction = cur_record['direction']
                                    execute_price = cur_record['execute_price']
                                    reason = cur_record['reason']
                                    status = cur_record['status']
                                    profit = cur_record['profit']
                                    mul = cur_record['mul']
                                    open_time = cur_record['open_time']
                                    close_time = cur_record['close_time']
                                    
                                    stat_funds += profit * mul
                                    max_funds = max(max_funds, stat_funds)
                                    min_funds = min(min_funds, stat_funds)
                                    
                                    profit_list.append(profit)
                                    trade_count += 1
                                    if profit > 0:
                                        win_count += 1
                                    elif profit < 0:
                                        loss_count += 1
                                    
                                    if direction == 1:
                                        long_count += 1
                                    elif direction == -1:
                                        short_count += 1
                                    
                        funds_curve_list.append(stat_funds)
                    
                    monthly_funds = []
                    for month in month_dict:
                        cur_month_funds = self.init_funds
                        for cur_record in month_dict[month]:
                            
                            mul = float(cur_month_funds) / float(self.interleave)
                            if mul > self.maximum_lot_size:
                                mul = self.maximum_lot_size
                            elif mul < 0.01:
                                mul = 0.01
                                
                            profit = cur_record['profit']
                            cur_month_funds += profit * mul
                        monthly_funds.append(cur_month_funds)
                        
                    good_counter = 0
                    for funds in monthly_funds:
                        if funds > self.init_funds:
                            good_counter += 1
                    
                    weight = str(good_counter)
                        
                    max_gain = max(max_gain, stat_funds)
                    
                    print(time.time() - start_time, weight, max_gain, stat_funds, strategy_dict)
                    the_median = 'NA'
                    if len(profit_list) > 0:
                        the_median = "{:0.2f}".format(median(profit_list))
                    writter.write(weight + '\t' + "{:0.2f}".format(stat_funds) + '\tMMSM: ' + "{:0.2f}".format(min_funds) + ', ' + "{:0.2f}".format(max_funds) + ', ' + "{:0.2f}".format(sum(profit_list)) + ', ' + str(the_median) + '\tTC: ' + str(trade_count) + '\tWL' + str(win_count) + ',' + str(loss_count) + '\tLS' + str(long_count) + ',' + str(short_count) + '\t$$$' + '  '.join('{}, {}'.format(key, val) for key, val in sorted(strategy_dict.items())) + '\n')
                    final_val = '-9999999'
                    if len(funds_curve_list) > 0:
                        final_val = "{:0.2f}".format(funds_curve_list[-1])
                    writter_seq.write(final_val + '\tBG\t' + '\t'.join(["{:0.2f}".format(elem) for elem in funds_curve_list]) + '\n')
                    #print(strategy_dict)
                index += 1

if __name__ == '__main__':
    
    
    hour_mapper_seq = {
        14:0,
        15:1,
        16:2,
        17:3,
        18:4,
        19:5,
        20:6,
        21:7,
        22:8,
        23:9,
        0:10,
        1:11,
        2:12,
        3:13,
        4:14,
        5:15,
        6:16,
        7:17,
        8:18,
        9:19,
        10:20,
        11:21,
        12:22,
        13:23,
    }
    sim_card_file = str(sys.argv[1])
    output_folder = str(sys.argv[2])
    output_folder_seq = str(sys.argv[3])
    data_mode = str(sys.argv[4])
    target_year = int(sys.argv[5])
    pk_file = str(sys.argv[6])
    day_search_file = str(sys.argv[7])
    
    formatter = '%Y/%m/%d-%H:%M:%S'
    
    BackTester = BackTester()
    start_time = time.time()
    BackTester.process()
    print('Time cost:', time.time() - start_time)

