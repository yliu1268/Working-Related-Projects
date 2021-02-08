import os, pickle, sys
from datetime import datetime
from datetime import timedelta

input_file = '/Users/yjing/projects/s00_data/GBPJPY.scid_BarData_5min_20201112_HL3_ranger.txt'
output_pk_file = '/Users/yjing/projects/s00_data/data_dict.pk'
output_pk_file_sequence_HL_index_file = '/Users/yjing/projects/s00_data/HL_index_table.pk'
formatter = '%Y/%m/%d-%H:%M:%S'


def save_obj(obj, obj_name):
    with open(obj_name, 'wb') as f:
        pickle.dump(obj, f, pickle.HIGHEST_PROTOCOL)
        
###
if os.path.isfile(output_pk_file):
    os.remove(output_pk_file)
    print('output_pk_file', output_pk_file, 'been removed.')

if os.path.isfile(output_pk_file_sequence_HL_index_file):
    os.remove(output_pk_file_sequence_HL_index_file)
    print('output_pk_file_sequence_HL_index_file', output_pk_file_sequence_HL_index_file, 'been removed.')
###

print('Is building the data content tree...')
data_dict = {}
sequence_HL_dict = {}
with open(input_file, 'r') as f:
    for line in f:
        line_list = line.strip('\n').split(',')
        if line_list[0] != 'Date':
            
            # Date, Time, Open, High, Low, Last, Volume, NumberOfTrades, BidVolume, AskVolume
            # 2007/12/3, 04:00:00, 227.867, 228.238, 227.651, 227.825, 1775, 1775, 0, 0
            
            Time = line_list[1].strip()
            line_dict = {
                'Date': line_list[0].strip(),
                'Time': Time,
                'Open': float(line_list[2]),
                'High': float(line_list[3]),
                'Low': float(line_list[4]),
                'Close': float(line_list[5]),
                'Hour': int(Time.split(':')[0].strip()),
                'Minute': int(Time.split(':')[1].strip()),
                'Raw_index': int(line_list[-4]),
                'HL1': line_list[-3],
                'HL2': line_list[-2],
                'HL3': line_list[-1]
                }
            # %Y%m/%d-%H:%M:%S
            
            Date_obj = datetime.strptime(line_dict['Date'] + '-' + line_dict['Time'], formatter) - timedelta(hours=14)
            
            cur_day = str(Date_obj).split(' ')[0]
            
            if cur_day not in data_dict:
                data_dict[cur_day] = {}
                data_dict[cur_day]['CONTENT'] = []
                
            data_dict[cur_day]['CONTENT'].append(line_dict)
            sequence_HL_dict[line_dict['Raw_index']] = {}
            sequence_HL_dict[line_dict['Raw_index']]['CONTENT'] = line_dict
            
print('Is adding OHLC info...')
for day in data_dict:
    
    cur_day_content = data_dict[day]['CONTENT']
    
    cur_day_high = -1.000
    cur_day_low = 9999.999
    high_time = -1
    low_time = -1
    
    for obj in cur_day_content:
        Close = obj['Close']
        Time = obj['Time']
        Date = obj['Date']
        
        cur_time = Date + ' ' + Time
        
        if Close > cur_day_high:
            cur_day_high = Close
            high_time = cur_time
            
        if Close < cur_day_low:
            cur_day_low = Close
            low_time = cur_time
            
    data_dict[day]['OHLC_D'] = {
        'Open_D': float(cur_day_content[0]['Open']),
        'High_D': cur_day_high,
        'Low_D': cur_day_low,
        'Close_D': float(cur_day_content[-1]['Close']),
        'Open_Time_D': cur_day_content[0]['Time'],
        'Close_Time_D': cur_day_content[-1]['Time'],
        'High_Time_D': high_time,
        'Low_Time_D': low_time,
    }
    
    if high_time == -1 or low_time == -1:
        print('System ERROR!!')

print('Is adding dynamic HL info...')
for day in data_dict:
    cur_day_content = data_dict[day]['CONTENT']
    data_dict[day]['DYNAMIC_HL'] = {}
    for i in range(len(cur_day_content)):
        high_val = -1
        low_val = 9999.999
        high_index = -1
        low_index = -1
        for j in range(0, i - 1):
            prev_close = cur_day_content[j]['Close']
            if prev_close > high_val:
                high_val = prev_close
                high_index = j
                
            if prev_close < low_val:
                low_val = prev_close
                low_index = j
        
        data_dict[day]['DYNAMIC_HL'][i] = {
            'HIGH_DYN': high_val,
            'HIGH_INDEX': high_index,
            'LOW_DYN': low_val,
            'LOW_INDEX': low_index
        }

print('Is dumping the data into driver...')
save_obj(data_dict, output_pk_file)

print('Is creating the sequence index table...')
sequence_index_list = sorted(sequence_HL_dict)
for sequence_index in sequence_index_list:
    line_dict = sequence_HL_dict[sequence_index]['CONTENT']
    
    if sequence_index > 0 and sequence_index < sequence_index_list[-1]:
        
        H_index = -1
        L_index = -1
        for index_prev in range(sequence_index - 1, -1, -1):
            prev_line_dict = sequence_HL_dict[index_prev]['CONTENT']
            HL3 = prev_line_dict['HL3']
            
            if '#' in HL3:
                if H_index == -1 and HL3.split('#')[0] == 'H3':
                    H_index = index_prev
                    
                if L_index == -1 and HL3.split('#')[0] == 'L3':
                    L_index = index_prev
                    
            if H_index != -1 and L_index != -1:
                break
        
        sequence_HL_dict[sequence_index]['H3_prev'] = H_index
        sequence_HL_dict[sequence_index]['L3_prev'] = L_index
        
        if sequence_index % 10000 == 0:
            print(sequence_index, H_index, L_index, sequence_HL_dict[sequence_index]['CONTENT']['Date'], sequence_HL_dict[sequence_index]['CONTENT']['Time'])

print('Is dumping the HL3 data into driver...')
save_obj(sequence_HL_dict, output_pk_file_sequence_HL_index_file)

print('All finished!')

