import pandas as pd
import csv
import os
import sys
import subprocess
import re
import itertools as it
import gc
import time

sys.path.append('/home/markhuberty/Documents/psClean/code')
import psCleanup

os.chdir('/home/markhuberty/Documents/psClean/')

## Define some useful functions
def count_green_patents(patent_ipcs, patent_year, green_cat_regex, country):
    """
    Counts patents by category
    Args:
       patent_ipcs: list of asterisk-separated ipcs by patent
       patent_year: list of filing years by patent
       green_cat_regex: a dictionary of regular expressions of the form
         category:regex
       country: the geographic entity in which these patents were filed
    Output:
       Counts of patents by category and year, as a data frame of shape
       year * category
    """
    cat_counts = {}
    for gcr in green_cat_regex:
        regexp = green_cat_regex[gcr]
        cat_count = []
        for ipc in patent_ipcs:
#            print ipc
            if type(ipc) is str:
                if regexp.search(ipc):
                    this_cat_count = 1
                else:
                    this_cat_count = 0
            else:
                this_cat_count = 0
            cat_count.append(this_cat_count)
        cat_counts[gcr] = cat_count

    print 'counts generated'
    cat_counts['year'] = patent_year
    df_counts = pd.DataFrame(cat_counts)
    grouped_counts = df_counts.groupby('year')
    summed_counts = grouped_counts.agg(sum)
    summed_counts['country'] = country
    return summed_counts

country_files = os.listdir('./data')
country_files = [f for f in country_files if 'tsv' in f]


## Read in and group the gre en IPC codes per the WIPO definition
green_ipcs = pd.read_csv('./data/ipc_green_inventory_tags_8dig.csv')

## Clean the ipc codes to match those in the PATSTAT output
ipc_clean = psCleanup.ipc_clean(green_ipcs['ipc'])
green_ipcs['ipc'] = ipc_clean
del ipc_clean

## Categorize at the top level the IPC codes
green_energy_cats = {}
for idx, d in enumerate(green_ipcs.l1):
    if d in green_energy_cats:
        green_energy_cats[d].append(green_ipcs.ipc[idx])
    else:
        green_energy_cats[d] = [green_ipcs.ipc[idx]]

## Translate the ipc codes into regex for searching
cat_regex = psCleanup.make_regex(green_energy_cats)

## Clean the data

country_files = os.listdir('./data')
country_files = [f for f in country_files if 'tsv' in f]

## Drop the -no country- file, very very large (32m rows)
country_files = [f for f in country_files if ' ' not in f]

for idx, f in enumerate(country_files):
    input_filename = './data/' + f
    output_filename = './data/cleaned_data/' + f

    print 'Operating on ' + input_filename
    
    reader_conn = open(input_filename, 'rt')
    writer_conn = open(output_filename, 'wt')
    reader = csv.reader(reader_conn, delimiter='\t')
    writer = csv.writer(writer_conn)

    for row in reader:

        if len(row) not in [10,11]:
            print len(row)
        
        if len(row) == 10:
            del row[0]
        else:
            del row[0]
            del row[0]
        writer.writerow(row)
    reader_conn.close()
    writer_conn.close()

    
        
