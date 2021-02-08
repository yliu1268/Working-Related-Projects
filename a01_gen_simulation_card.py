import time, shutil, os
from random import shuffle


base_dir = os.path.dirname(os.path.realpath(__file__))

output_dir = base_dir + '/simulation_cards'

###
shutil.rmtree(output_dir, ignore_errors = True)
os.makedirs(output_dir)
###

start_time = time.time()
index = 0
cut_length = 30000
file_set = set()
out_list = []

for STOP in ['F#2000', 'F#3000', 'HL#0', 'HL#200']:
    for hour_S1 in [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23]:
        for hour_S3 in range(hour_S1, 23 + 1):
            for trailing_mode in [0]:
                for target_profit in ['0#0']:
                    for direction_flip_1 in [-1, 1]:
                        for direction_flip_2 in [-1, 1]:
                            for flip_operation_1 in [0, 1]:
                                for flip_operation_2 in [0, 1]:
                                    for decision_type_1 in [1, 2, 3]:
                                        for decision_type_2 in [1, 2, 3]:
                                            for target_close_FMT in [0, 1, 2, 3, 4, 5]:
                                                for flip_1 in [-1, 1]:
                                                    for flip_2 in [-1, 1]:
                                                        strategy_dict = {
                                                            'STOP': STOP,
                                                            'trailing_mode': trailing_mode,
                                                            'target_profit': target_profit,
                                                            'hour_S1': hour_S1,
                                                            'hour_S3': hour_S3,
                                                            'direction_flip_1': direction_flip_1,
                                                            'direction_flip_2': direction_flip_2,
                                                            'flip_operation_1': flip_operation_1,
                                                            'flip_operation_2': flip_operation_2,
                                                            'decision_type_1': decision_type_1,
                                                            'decision_type_2': decision_type_2,
                                                            'target_close_FMT': target_close_FMT,
                                                            'flip_1': flip_1,
                                                            'flip_2': flip_2,
                                                        }
                                                        tailing = str(int(float(index) / float(cut_length)))
                                                        output_file = output_dir + '/simulation_card_' + tailing + '.txt'
                                                        
                                                        if output_file not in file_set:
                                                            writter = open(output_file, "a")
                                                            out_str = ''
                                                            for feature_name in sorted(strategy_dict):
                                                                out_str += feature_name + ','
                                                            writter.write(out_str.strip(',') + '\n')
                                                        
                                                        if index % 100000 == 0:
                                                            print(index, time.time() - start_time, strategy_dict)
                                                        
                                                        out_str = ''
                                                        for feature_name in sorted(strategy_dict):
                                                            out_str += str(strategy_dict[feature_name]) + ','
                                                        writter.write(out_str.strip(',') + '\n')
                                                            
                                                        index += 1
                                                        file_set.add(output_file)
                                                        


print('Total time cost:', time.time() - start_time)

