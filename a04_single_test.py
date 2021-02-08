from datetime import datetime
from datetime import timedelta
import sys, pickle, time
import os.path

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
        
        stopLoss = 600
        trailing_mode = 0
        target_profit_mode = 0
        target_profit = 0
        hour_S1 = 3
        hour_S3 = 11
        direction_flip = -1
        target_market_type = 4
        second_flip = -1
        decision_type_1 = 1
        decision_type_2 = 3
        
        flip_operation_0 = 0
        flip_operation_1 = 1
        
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
                
                Open_daily_prev = data_dict[prev_day]['OHLC_D']['Open_D']
                High_daily_prev = data_dict[prev_day]['OHLC_D']['High_D']
                Low_daily_prev = data_dict[prev_day]['OHLC_D']['Low_D']
                Close_daily_prev = data_dict[prev_day]['OHLC_D']['Close_D']
                Open_time_prev = data_dict[prev_day]['OHLC_D']['Open_Time_D']
                Close_time_prev = data_dict[prev_day]['OHLC_D']['Close_Time_D']
                
                Open_daily_cur = data_dict[day]['OHLC_D']['Open_D']
                High_daily_cur = data_dict[day]['OHLC_D']['High_D']
                Low_daily_cur = data_dict[day]['OHLC_D']['Low_D']
                Close_daily_cur = data_dict[day]['OHLC_D']['Close_D']
                Open_time_cur = data_dict[day]['OHLC_D']['Open_Time_D']
                Close_time_cur = data_dict[day]['OHLC_D']['Close_Time_D']
                
                prev_body_2 = Close_daily_prev_2 - Open_daily_prev_2
                prev_body = Close_daily_prev - Open_daily_prev
                
                market_type = 0
                # No1
                if High_daily_prev_2 > High_daily_prev and Low_daily_prev_2 < Low_daily_prev:
                    market_type = 1
                # No2
                elif High_daily_prev_2 < High_daily_prev and Low_daily_prev_2 > Low_daily_prev:
                    market_type = 2
                # No3
                elif High_daily_prev_2 > High_daily_prev and Low_daily_prev_2 > Low_daily_prev:
                    market_type = 3
                # No4
                elif High_daily_prev_2 < High_daily_prev and Low_daily_prev_2 < Low_daily_prev:
                    market_type = 4
                
                if Close_time_cur == '13:55:00' and Open_time_cur == '14:00:00':
                    
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
                        
                        #cur_hour4 = hour_mapper2[int(cur_hour)]
                        cur_hour4 = hour_mapper_int[int(cur_hour)]
                        #cur_hour4 = hour_mapper_AEU[int(cur_hour)]
                        #cur_hour4 = hour_mapper[int(cur_hour)]
                        #cur_hour4 = int(cur_hour)
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
                        
                        prev_body = Close_daily_prev - Open_daily_prev
                        
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
                            
                        if len(trading_records[day]) == 0:
                            if market_type == target_market_type:
                                if the_decision_1 == -1 and cur_hour4 == hour_S1:
                                    if flip_operation_0 == 1:
                                        order_type = 'REAL'
                                        status = 'open'
                                    else:
                                        order_type = 'PLACE_HOLDER'
                                        status = 'C'
                                        
                                    open_info = {
                                        'direction': 1 * direction_flip,
                                        'execute_price': Close,
                                        'reason': 'E1',
                                        'order_type': order_type,
                                        'status': status,
                                        'profit': 0,
                                        'mul': mul,
                                        'open_time': Date + ' ' + Time,
                                        'stop_price': Close - direction_flip * (float(stopLoss) / 1000.0),
                                        'max_gain_val': -9999.999,
                                        'pnl_list': []
                                    }
                                    trading_records[day].append(open_info)
                                elif the_decision_1 == 1 and cur_hour4 == hour_S1:
                                    if flip_operation_0 == 1:
                                        order_type = 'REAL'
                                        status = 'open'
                                    else:
                                        order_type = 'PLACE_HOLDER'
                                        status = 'C'
                                        
                                    open_info = {
                                        'direction': -1 * direction_flip,
                                        'execute_price': Close,
                                        'reason': 'E2',
                                        'order_type': order_type,
                                        'status': status,
                                        'profit': 0,
                                        'mul': mul,
                                        'open_time': Date + ' ' + Time,
                                        'stop_price': Close + direction_flip * (float(stopLoss) / 1000.0),
                                        'max_gain_val': -9999.999,
                                        'pnl_list': []
                                    }
                                    trading_records[day].append(open_info)
                                        
                        else:
                            if trading_records[day][-1]['reason'] == 'E1' and the_decision_2 == second_flip and cur_hour4 == hour_S3:
                                if trading_records[day][-1]['status'] == 'open':
                                    if trading_records[day][-1]['order_type'] == 'REAL':
                                        trading_records[day][-1]['profit'] = direction_flip * 1000.0 * (Close - trading_records[day][-1]['execute_price']) - self.spread
                                        trading_records[day][-1]['status'] = 'close'
                                        trading_records[day][-1]['close_price'] = Close
                                        trading_records[day][-1]['close_time'] = Date + ' ' + Time
                                        trading_records[day][-1]['close_reason'] = 'FP1'
                                    else:
                                        trading_records[day][-1]['profit'] = 0
                                        trading_records[day][-1]['status'] = 'close'
                                        trading_records[day][-1]['close_price'] = Close
                                        trading_records[day][-1]['close_time'] = Date + ' ' + Time
                                        trading_records[day][-1]['close_reason'] = 'FP2'
                                        
                                if flip_operation_1 == 1:
                                    open_info = {
                                        'direction': -1 * direction_flip,
                                        'execute_price': Close,
                                        'reason': 'E3',
                                        'order_type': 'REAL',
                                        'status': 'open',
                                        'profit': 0,
                                        'mul': mul,
                                        'open_time': Date + ' ' + Time,
                                        'stop_price': Close + direction_flip * (float(stopLoss) / 1000.0),
                                        'max_gain_val': -9999.999,
                                        'pnl_list': []
                                    }
                                    trading_records[day].append(open_info)
                            elif trading_records[day][-1]['reason'] == 'E2' and the_decision_2 == -second_flip and cur_hour4 == hour_S3:
                                if trading_records[day][-1]['status'] == 'open':
                                    if trading_records[day][-1]['order_type'] == 'REAL':
                                        trading_records[day][-1]['profit'] = -direction_flip * 1000.0 * (Close - trading_records[day][-1]['execute_price']) - self.spread
                                        trading_records[day][-1]['status'] = 'close'
                                        trading_records[day][-1]['close_price'] = Close
                                        trading_records[day][-1]['close_time'] = Date + ' ' + Time
                                        trading_records[day][-1]['close_reason'] = 'FP3'
                                    else:
                                        trading_records[day][-1]['profit'] = 0
                                        trading_records[day][-1]['status'] = 'close'
                                        trading_records[day][-1]['close_price'] = Close
                                        trading_records[day][-1]['close_time'] = Date + ' ' + Time
                                        trading_records[day][-1]['close_reason'] = 'FP4'
                                    
                                if flip_operation_1 == 1:
                                    open_info = {
                                        'direction': 1 * direction_flip,
                                        'execute_price': Close,
                                        'reason': 'E4',
                                        'order_type': 'REAL',
                                        'status': 'open',
                                        'profit': 0,
                                        'mul': mul,
                                        'open_time': Date + ' ' + Time,
                                        'stop_price': Close - direction_flip * (float(stopLoss) / 1000.0),
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
        
        target_year = 2011
        data_dict = self._load_obj(pk_file)
        hl_index_table = self._load_obj(hl_index_table_file)
        day_search_dict = self._load_obj(day_search_file)
        filted_data_dict = {}
        for day in data_dict:
            year = day.split('-')[0]
            #if int(year) == target_year:
            filted_data_dict[day] = data_dict[day]
        
        print('filted_data_dict', len(filted_data_dict), len(data_dict))
        
        max_gain = -1
        index = 0
                
        start_time = time.time()
        
        strategy_dict = {}

        trading_records = self._exe_cur_strategy(strategy_dict, filted_data_dict, day_search_dict)
        index = 0
        stat_funds = self.init_funds
        month_dict = {}
        stat_dict = {}
        for day in sorted(trading_records):
            
            month = day.split('-')[1]
            if month not in month_dict:
                month_dict[month] = []
            
            
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
                    close_price = cur_record['close_price']
                    close_reason = cur_record['close_reason']
                    #max_gain_val = cur_record['max_gain_val']
                    
                    # overwrite the mul info.
                    """
                    mul = float(stat_funds) / float(self.interleave)
                    if mul > self.maximum_lot_size:
                        mul = self.maximum_lot_size
                    elif mul < 0.01:
                        mul = 0.01
                    """
                    #if profit < 0:
                    #    profit = max_gain_val / 2.0
                    
                    open_hour = int(open_time.split(' ')[1].split(':')[0])
                    #if open_hour in [3, 21, 20, 17, 16, 15]:
                    stat_funds += profit * mul
                    
                    
                    #if profit < 0:
                    if open_hour not in stat_dict:
                        stat_dict[open_hour] = []
                    stat_dict[open_hour].append(profit)
                    print(str(index) + ',' + day + ',' + str(stat_funds) + ',' + str(profit) + ',' + str('') + ',' + str(mul) + ',' + str(status) + ',' + str(direction) + ',' + close_reason + ',' + reason + ',' + str(execute_price) + ',' + str(close_price) + ',' + str(open_time) + ',' + str(close_time))
                    #print(cur_record['pnl_list'])
                    
                    index += 1
        
        monthly_funds = []
        for month in sorted(month_dict):
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
            if funds > self.init_funds * 1.0:
                good_counter += 1
        if good_counter == len(monthly_funds):
            print('gd', monthly_funds)
        else:
            print('bad', monthly_funds)
        
        max_gain = max(max_gain, stat_funds)
        print(time.time() - start_time, max_gain, stat_funds, strategy_dict)
        
        for hout in stat_dict:
            print(hout, len(stat_dict[hout]), sum(stat_dict[hout]))
        #print(strategy_dict)

if __name__ == '__main__':
    
    hour_mapper_int = {
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
        4:24,
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
    hour_mapper = {
        2:1,
        3:1,
        4:1,
        5:1,
        6:2,
        7:2,
        8:2,
        9:2,
        10:3,
        11:3,
        12:3,
        13:3,
        14:4,
        15:4,
        16:4,
        17:4,
        18:5,
        19:5,
        20:5,
        21:5,
        22:6,
        23:6,
        0:6,
        1:6
    }
    
    pk_file = '/Users/yjing/projects/s00_data/data_dict.pk'
    hl_index_table_file = '/Users/yjing/projects/s00_data/HL_index_table.pk'
    formatter = '%Y/%m/%d-%H:%M:%S'
    day_search_file = '/Users/yjing/projects/s00_data/day_search.pk'

    BackTester = BackTester()
    start_time = time.time()
    BackTester.process()
    print('Time cost:', time.time() - start_time)

