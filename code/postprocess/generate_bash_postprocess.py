import sys
import os
import re

re_country = re.compile('\_([a-z]{2})\.csv')

inputs = [i for idx, i in enumerate(sys.argv) if idx > 0]
input_dir = inputs[0]
output_dir = inputs[1]

input_files = os.listdir(input_dir)
input_csv_files = [i for i in input_files if 'csv' in i]


countries = [re_country.search(i).group(1).lower()
             for i in input_csv_files]

base_str = 'python map_dedupe_han_leuven.py ' + \
           input_dir + '%s %s ' + \
           output_dir


with open('eu27_postprocess_script.sh', 'a') as f:
    f.write('#!/bin/bash')
    f.write('\n')

for c, i in zip(countries, input_csv_files):
    output_str = base_str % (i, c)
    print output_str
    with open('eu27_postprocess_script.sh', 'a') as f:
        f.write(output_str)
        f.write('\n')

    
    
