#!/bin/sh

pgm=validation_helper

python=/usr/bin/python3

curwkDB=$PWD

data_mode=YEAR
target_year=2020
simulation_cards_folder=${curwkDB}/simulation_cards
validation_file=${curwkDB}/cur_str_stat.txt
output_folder=${curwkDB}/val
pk_file=/Users/yjing/projects/s00_data/one_hour/data_dict.pk
day_search_file=/Users/yjing/projects/s00_data/one_hour/day_search.pk
num_thread=8

rm -rf ${output_folder}
mkdir -p ${output_folder}

SECONDS=0
##############################################
echo 'Starting to process files...'

${python} -u ${pgm}.py ${validation_file} ${output_folder} ${data_mode} ${target_year} ${pk_file} ${day_search_file} > ${output_folder}/${pgm}.log

echo "All finished!!"

duration=$SECONDS
echo "$(($duration / 60)) minutes and $(($duration % 60)) seconds elapsed "
