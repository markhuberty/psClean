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

def get_non_overlapping_ngrams(name_string, ngram_length):
    i = 0
    these_ngrams = []
    while i < (len(name_string) - ngram_length):
        these_ngrams.append(''.join(name_string[j] for j in range(i, i + ngram_length)))
        i += ngram_length
    return set(these_ngrams)
        
def get_combinatorial_pairs(ngram_list):
    pairs = []
    for i, n in enumerate(ngram_list):
        for j in range(i + 1, len(ngram_list)):
            pair = tuple(sorted([n, ngram_list[j]]))
            if pair not in pairs:
                pairs.append(pair)
    return(pairs)


## Generate the file names
base_path = '/mnt/db_master/patstat_raw/dvd1/'
base_name = 'tls206_part0'
base_suffix = '_clean.txt.namestd'

file_names = [base_path + base_name + str(i) + base_suffix for
              i in range(1,6)]

## Load up the IDs I am searching for
id_file = './data/1000_cp_cids_dsort_uniq.csv'

with open(id_file, 'rt') as f:
    reader = csv.reader(f)
    id_list = [row for row in reader]

## Ccnvert the ID values to numbers
int_ids = []
canonical_names = []
for entry in id_list:
    canonical_names.append(entry[0])
    raw_ids = entry[1]
    raw_ids = re.sub('set\(\[', '', raw_ids)
    raw_ids = re.sub('\]\)', '', raw_ids)
    raw_ids = re.sub('u', '', raw_ids)
    raw_ids = re.sub("\'", '', raw_ids)
    raw_ids = re.split(',', raw_ids)
    raw_ids = [int(r.strip()) for r in raw_ids]
    int_ids.extend(raw_ids)

int_ids.sort()



ngram_length = 3
## Get the primary hashes
company_name_hashes = {}
for cn in canonical_names:
    cluster_ngrams = get_non_overlapping_ngrams(cn, ngram_length=ngram_length)
    print cluster_ngrams
    company_name_hashes[cn] = cluster_ngrams

def len_summary(match_dict):
    len_vals = [len(v) for k,v in match_dict.iteritems()]
    max_len = max(len_vals)
    min_len = min(len_vals)
    mean_len = mean(len_vals)
    print min_len, mean_len, max_len
    return 0
## Get the set of potential matches for each company based on
## the primary hash
    
company_potential_matches = {}
leading_n = 3
for f_idx, f in enumerate(file_names):
    print(f)
    conn = open(f, 'rt')
    reader = csv.DictReader(conn)
    t0 = time.time()
    for idx, row in enumerate(reader):
        if idx % 10000 == 0 and idx > 0:
            t1 = time.time()
            time_diff = t1 - t0
            print idx, time_diff / idx
            len_summary(company_potential_matches)
        name = row['PERSON_NAME']
        name_ngrams = get_non_overlapping_ngrams(name,
                                                 ngram_length=ngram_length
                                                 )
        ## name_hashes = get_combinatorial_pairs(name_ngrams)
        for c in company_name_hashes:
            hash_overlap = company_name_hashes[c] & name_ngrams
            if len(hash_overlap) > 2:
                if c in company_potential_matches:
                    company_potential_matches[c].append(name)
                else:
                    company_potential_matches[c] = [name]
            else:
                continue
    conn.close()


## Now cluster by name. Need to block within each chunk first
leading_n = 2
ngram = 2
threshold = 0.6
for k in company_potential_matches:
    t0 = time.time()
    block_dict = {}
    for name in company_potential_matches[k]:
        leading_letter_hash = name[0:leading_n]
        if leading_letter_hash in block_dict:
            block_dict[leading_letter_hash].append(name)
        else:
            block_dict[leading_letter_hash] = [name]
    match_list = []
    for block in block_dict:
        ## Build up the ngram matrix for this canonical name and
        ## all potential matches
        this_block = block_dict[block]
        whole_block = this_block[:]
        whole_block.append(k)
        name_row = len(whole_block) - 1
        mat = psDisambig.build_incremental_ngram_mat(whole_block,
                                                     n=ngram
                                                     )
        if mat['tf_matrix'] is not None:
            if mat['tf_matrix'].shape[0] > 1:
                ## COO matrix allows better slicing
                mat['tf_matrix'] = mat['tf_matrix'].tocoo()
                sim = psDisambig.rowwise_cosine_similarity(mat['tf_matrix'],
                                                           mat['tf_matrix'].getrow(name_row)
                                                           )
                for colnum in range(sim.shape[1]):
                    if sim[0, colnum] > threshold and colnum != name_row:
                        match = [(whole_block[colnum], sim[0, colnum])]
                         if len(match) > 0:
                            match_list.extend(match)
        
    filename = './data/candidate_matches/' + k + '_' + str(threshold) + '_candidate_match_' + '23_07_2012.csv'
    with open(filename, 'wt') as f:
        writer = csv.writer(f)
        for match in match_list:
            writer.writerow(match)
    print 'Finished'
    print k
    print time.time() - t0
