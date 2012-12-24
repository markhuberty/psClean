import csv
import random


## Define the resevoir sampling function
def resevoir_sample(file_dictreader, n_lines):
    output = [0] * n_lines
    line_count = 0
    for row in file_dictreader:
        line_count += 1
        addr = row['person_address']
        if addr is not null and addr !='':
            if line_count < n_lines:
                output[line_count] = addr
            else if random.randint(0, line_count) <= n_lines:
                n_addr = len(output)
                idx = random.randint(0, n_addr)
                output[idx] = addr
        else:
            continue
    return output

os.chdir('/home/markhuberty/Documents/psClean/')


## Acquire and subset data sources
datadir = './data/cleaned_data'
country_files = os.listdir(datadir) ## fix this
country_files = [f for f in country_files if 'tsv' in f and ' ' not in f]

##
country_list = ['DE', 'US', 'KR', 'FR', 'GB', 'DK', 'IT',
                'CN', 'CA', 'IN']


n_addresses_by_country = 500

addresses = {}
for f in country_files:
    conn = open(f, 'rt')
    reader = csv.DictReader(conn)

    for row in reader:
        this_country = row['person_ctry_cde']
        sample_this_country = this_country in country_list

        if sample_this_country:
            addresses[this_country] = resevoir_sample(reader,
                                                      n_addresses_by_country
                                                      )
        else:
            break
        
    
        
