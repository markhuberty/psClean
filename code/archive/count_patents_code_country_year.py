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
   
    filename = './data/cleaned_data/' + f
    print 'Counting patents in ' + filename

    
    df = pd.read_csv(filename)
    print df.shape

    df.columns = ['appln_id',
                  'person_id',
                  'person_name',
                  'person_address',
                  'person_ctry_code',
                  'firm_legal_id',
                  'coauthors',
                  'ipc_code',
                  'year'
                  ]
    start_time = time.time()
    
    # Get country-level patents (not country-individual patents)
    cols = ['appln_id', 'person_ctry_code', 'ipc_code', 'year']
    df_country_ipcs = df[cols].drop_duplicates()

    del df
    gc.collect()

    country = filename[-6:-4] ## Check this
    
    ## Count
    green_cat_count = count_green_patents(df_country_ipcs['ipc_code'],
                                          df_country_ipcs['year'],
                                          cat_regex,
                                          country
                                          )
    green_cat_count = green_cat_count.reset_index()

    df_country_ipcs['identity'] = 1

    patents_grouped = df_country_ipcs.groupby('year')
    total_patents = patents_grouped['identity'].agg(sum)
    total_patents_df = pd.DataFrame({'country':country, 'patent_count':total_patents})
    total_patents_df = total_patents_df.reset_index()

    # Accumulate the country-year-count dataframe
    if idx == 0:
        all_green_counts = green_cat_count
        all_total_counts = total_patents_df
    else:
        all_green_counts = all_green_counts.append(green_cat_count, ignore_index=True)
        all_total_counts = all_total_counts.append(total_patents_df, ignore_index=True)
    end_time = time.time()
    record_count = len(df_country_ipcs)

    if record_count > 0:
        time_per_record = (end_time - start_time) / record_count
        print 'Cleaning time per record: ' + str(time_per_record)
    del df_country_ipcs
    del total_patents
    del green_cat_count
    
all_green_counts.to_csv('./data/country_green_patent_counts_byyear.csv')
all_total_counts.to_csv('./data/country_total_patent_counts_byyear.csv')

    
        
