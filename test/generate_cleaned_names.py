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

initial_query = """
SELECT person_id, person_name from (tls201_appln INNER JOIN
tls207_pers_appln USING(appln_id)) INNER JOIN
tls206_person USING(person_id)
WHERE YEAR(appln_filing_date) >= 1980
"""

subsequent_query = """
SELECT person_id, person_name from (tls201_appln INNER JOIN
tls207_pers_appln USING(appln_id)) INNER JOIN
tls206_person USING(person_id)
WHERE YEAR(appln_filing_date) >= 1980
AND person_id > 
"""

## Load the sql query as an iterator to save on memory
conn = MySQLdb.connect(host="127.0.0.1",
                       port=3306,
                       user="markhuberty",
                       passwd="patstat_huberty",
                       db="patstatOct2011",
                       use_unicode=True,
                       charset='utf8',
                       cursorclass = MySQLdb.cursors.SSCursor
                       )

conn_cursor = conn.cursor()
conn_cursor.execute(initial_query)

## Open the connection and write each id + name combination to
## the file as it's cleaned.
# with open('../data/full_cleaned_name_list.csv', 'wt') as output_conn:
#     writer = csv.writer(output_conn)
#     total_time = 0
#     N = 0
#     max_id = 0
#     for row in conn_cursor:
#         t0 = time.time()
#         person_id, person_name = row
#         if len(person_name) > 1:
#             person_name = unicode(person_name)
#         else:
#             continue
#         clean_person_name = clean_wrapper(person_name, all_dicts)
#         clean_person_name = psCleanup.encoder(clean_person_name)
#         writer.writerow([person_id, clean_person_name])
#         t1 = time.time()
#         time_diff = t1 - t0
#         total_time += time_diff
#         N += 1
#         if N % 1000 == 0:
#             print N, total_time, total_time / N

with open('../data/full_cleaned_name_list.csv', 'wt') as output_conn:
    writer = csv.writer(output_conn)
    total_time = 0
    N = 0
    pid_range = []
    max_ids = 25000
    while True:
        try:
            row = conn_cursor.fetchone()
        except MySQLdb.OperationalError:
            ## DB connection lost, re-set it
            ## with maxid
            conn.close()
            conn = MySQLdb.connect(host="127.0.0.1",
                                   port=3306,
                                   user="markhuberty",
                                   passwd="patstat_huberty",
                                   db="patstatOct2011",
                                   use_unicode=True,
                                   charset='utf8',
                                   cursorclass = MySQLdb.cursors.SSCursor
                                   )
            this_query = subsequent_query + str(person_id)
            print 're-establishing connection'
            print this_query
            conn_cursor = conn.cursor()
            conn_cursor.execute(this_query)
            continue
        t0 = time.time()
        person_id, person_name = row
        ## Check the name validity and make sure that
        ## we haven't seen the person_id before
        if len(person_name) > 1 and person_id not in pid_range:
            person_name = unicode(person_name)
            pid_range.append(person_id)
            if len(pid_range) > max_ids:
                pid_range.pop(0)
        else:
            continue
        clean_person_name = clean_wrapper(person_name, all_dicts)
        clean_person_name = psCleanup.encoder(clean_person_name)
        writer.writerow([person_id, clean_person_name])
        t1 = time.time()
        time_diff = t1 - t0
        total_time += time_diff
        N += 1
        if N % 1000 == 0:
            print N, total_time, total_time / N


conn_cursor.close()
conn.close()
