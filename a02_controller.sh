#!/bin/sh

pgm=strategy_explore

python=/usr/bin/python3

curwkDB=$PWD

data_mode=YEAR
target_year=2019
simulation_cards_folder=${curwkDB}/simulation_cards
output_folder=${curwkDB}/output
output_folder_seq=${curwkDB}/output_seq
pk_file=/Users/yjing/projects/s00_data/one_hour/data_dict.pk
day_search_file=/Users/yjing/projects/s00_data/one_hour/day_search.pk
num_thread=8

rm -rf ${curwkDB}/Logs/
mkdir -p ${curwkDB}/Logs/

rm -rf ${output_folder}
mkdir -p ${output_folder}

rm -rf ${output_folder_seq}
mkdir -p ${output_folder_seq}

SECONDS=0
##############################################
echo 'Starting to process files...'
for file in ${simulation_cards_folder}/*.txt
do
	#####################
	while [ $(ps -fu yjing | grep ${pgm} | wc -l) -gt ${num_thread} ]; do
		sleep 1;
	done;
	#####################

	echo "${pgm} ${target_year} Is scanning file: $file"
	IFS='/' read -ra file_list <<< "$file"
	${python} -u ${pgm}.py ${file} ${output_folder} ${output_folder_seq} ${data_mode} ${target_year} ${pk_file} ${day_search_file} > ${curwkDB}/Logs/${pgm}_"${file_list[${#file_list[@]}-1]}".log &
	
done

wait

echo "All finished!!"

duration=$SECONDS
echo "$(($duration / 60)) minutes and $(($duration % 60)) seconds elapsed "
