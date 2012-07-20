import sys
sys.path.append('/home/markhuberty/Documents/psClean/code')
import psCleanup
import psDisambig
import MySQLdb
import MySQLdb.cursors
import re
import string
import numpy as np
import scipy.sparse as sp
import csv
import time
import os

os.chdir('/home/markhuberty/Documents/psClean/')


## Generate the file names
base_path = '/mnt/db_master/patstat_raw/dvd1/'
base_name = 'tls206_part0'
base_suffix = '_clean.txt.namestd'

file_names = [base_path + base_name + str(i) + base_suffix for
              i in range(0,6)]

## Load up the IDs I am searching for
id_file = './data/1000_cp_cids_dsort_uniq.csv'

with open(id_file, 'rt') as f:
    reader = csv.reader(f)
    id_list = [row for row in reader]

int_ids = []
for entry in id_list:
    name = entry[0]
    raw_ids = entry[1]
    raw_ids = re.sub('set\(\[', '', raw_ids)
    raw_ids = re.sub('\]\)', '', raw_ids)
    raw_ids = re.sub('u', '', raw_ids)
    raw_ids = re.sub("\'", '', raw_ids)
    raw_ids = re.split(',', raw_ids)
    raw_ids = [int(r.strip()) for r in raw_ids]
    int_ids.extend(raw_ids)

int_ids.sort()

## Scan across the files and take the names we care about
company_names = []
for f in file_names:
    conn = open(f, 'rt')
    reader = csv.DictReader(f)
    for row in reader:
        this_id = int(row['person_id'])
        if this_id in int_ids:
            company_names.append(row['person_name'])
        else:
            continue
    conn.close()

unique_names = list(set(company_names))

ngram_length = 2
## Get the primary hashes
company_name_hashes = {}
for c in company_names:
    cluster_ngrams = get_non_overlapping_ngrams(c, ngram_length=ngram_length)
    cluster_hash = get_combinatorial_pairs(cluster_ngrams)
    company_name_hashes[c] = cluster_hash

## Get the set of potential matches for each company based on
## the primary hash
company_potential_matches = {}
for f in files:
    conn = open(f, 'rt')
    reader = csv.DictReader(f)
    for row in reader:
        if row['person_id'] not in int_ids:
            name = row['person_name']
            name_ngrams = get_non_overlapping_ngrams(name, ngram_length=ngram_length)
            name_hashes = get_combinatorial_pairs(name_ngrams)
            
            for c in company_name_hashes:
                hash_overlap = set(company_name_hashes[c], name_hashes)
                if len(hash_overlap) > 0:
                    if c in company_potential_matches:
                        company_potential_matches[c].append(name)
                    else:
                        company_potential_matches[c] = [name]
    conn.close()

## Check the match length
for c in company_potential_matches:
    print c, len(company_potential_matches[c])
        
