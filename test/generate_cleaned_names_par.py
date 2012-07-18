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
from IPython.parallel import Client

## Define the cleaning dicts
all_dicts = [psCleanup.convert_html,
             psCleanup.convert_sgml,
             psCleanup.clean_symbols,
             psCleanup.concatenators,
             psCleanup.single_space,
             psCleanup.ampersand,
             psCleanup.us_uk,
             psCleanup.abbreviations
             ]

## Wrap the clean sequence in a useful function
def clean_wrapper(name_string, dict_list):
    out = psCleanup.rem_diacritics(name_string)
    out = psCleanup.stdize_case(out)
    out = psCleanup.master_clean_dicts([out], all_dicts)
    out = out[0].strip()
    return(out)

base_query = """
SELECT person_id, person_name from (tls201_appln INNER JOIN
tls207_pers_appln USING(appln_id)) INNER JOIN
tls206_person USING(person_id)
WHERE YEAR(appln_filing_date) >= 1980 LIMIT 
"""

## Run over the db 200k rows at a time
## For each chunk, parallelize over the rows to clean
## The write out each parallel chunk
## Proceed until no more rows are returned
## Keep ID window to ensure against dups

N=0
max_ids = 25000
block_size = 200000
row_idx = 0
max_seq_id = 

## Estimate the unique_id count:

conn = MySQLdb.connect(host="127.0.0.1",
                       port=3306,
                       user="markhuberty",
                       passwd="patstat_huberty",
                       db="patstatOct2011",
                       use_unicode=True,
                       charset='utf8'
                       )
conn_cursor = conn.cursor()
conn_cursor.execute('SELECT COUNT(DISTINCT(person_id)) from tls206_person)')
est_unique_ids = conn_cursor.fetchall()


while N <= est_unique_ids:
    this_query = base_query + str(row_idx) + ',' + str(row_idx + block_size)
    print this_query
    
    conn = MySQLdb.connect(host="127.0.0.1",
                           port=3306,
                           user="markhuberty",
                           passwd="patstat_huberty",
                           db="patstatOct2011",
                           use_unicode=True,
                           charset='utf8'
                           )
    
    conn_cursor = conn.cursor()
    conn_cursor.execute(this_query)
    db_response = con_cursor.fetchall()
    conn_cursor.close()
    conn.close()
    
    unique_id = []
    unique_name = []
    for person_id, person_name in db_response:
        is_unique = person_id not in unique_id
        is_long = len(unique_name) > 0
        is_next = person_id >= max_seq_id
        if is_unique and is_long and is_next:
            unique_id.append(person_id)
            unique_name.append(unicode(person_name))
    del db_response

    max_seq_id = max(unique_id)
    # Parallelize the clean step
    cleaned_rows = lview.map(clean_wrapper,
                             unique_name
                             )

    
    ## Write out the rows
    if N == 0:
        output_conn = open('../data/full_cleaned_name_list_par.csv', 'wt') 
    else:
        output_conn = open('../data/full_cleaned_name_list_par.csv', 'a') 
    writer = csv.writer(output_conn)
    writer.writerows(zip(unique_id,cleaned_rows))
    output_conn.close()

    N += block_size
    row_idx += block_size
    if N % 1000 == 0:
        print N
