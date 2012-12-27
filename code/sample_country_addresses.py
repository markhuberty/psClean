import csv
import os
import random


## Define the resevoir sampling function
def resevoir_sample(file_dictreader, n_lines):
    output = [0] * n_lines
    line_count = 0
    country = None
    for row_idx, row in enumerate(file_dictreader):
        #print row.keys()
        line_count += 1
        if row['person_ctry_code'] == 'person_ctry_code':
            continue
        elif country is None:
            country = row['person_ctry_code']
            print country
        addr = row['person_address']
        if addr is not None and addr !='':
            if line_count < n_lines:
                output[line_count] = addr
            elif random.randint(0, line_count) <= n_lines:
                n_addr = len(output)
                idx = random.randint(0, n_addr - 1)
                output[idx] = addr
        else:
            continue
    return country, output

os.chdir('/home/markhuberty/Documents/psClean/')


## Acquire and subset data sources
datadir = './data/cleaned_data'
country_files = os.listdir(datadir) ## fix this
country_files = [f for f in country_files if 'tsv' in f and ' ' not in f]

##
country_list = ['DE', 'US', 'KR', 'FR', 'GB', 'DK', 'IT',
                'CN', 'CA', 'IN']

fieldnames = ['appln_id',
              'person_id',
              'person_name',
              'person_address',
              'person_ctry_code',
              'firm_legal_id',
              'coauthors',
              'ipc_code',
              'year'
              ]

n_addresses_by_country = 200

addresses = {}
for f in country_files:
    full_path = datadir + '/' + f
    print full_path
    with open(full_path, 'rt') as conn:
        reader = csv.DictReader(conn, fieldnames=fieldnames)
        this_country, address_list = resevoir_sample(reader,
                                                     n_addresses_by_country
                                                     )
        addresses[this_country] = address_list

fieldnames = ['person_ctry_code', 'person_address']

output_path = datadir + '/sample_country_addresses.csv'
with open(output_path, 'wt') as conn:
    writer = csv.writer(conn)
    writer.writerow(fieldnames)
    for k in addresses:
        for row in addresses[k]:
            if row != 0:
                long_row = [k, row]
                writer.writerow(long_row)

    
        
