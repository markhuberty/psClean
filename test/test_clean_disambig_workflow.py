import sys
sys.path.append('/home/markhuberty/Documents/psClean/code')
import psCleanup
import psDisambig
import MySQLdb
import re
import time
import string
import numpy as np
import scipy.sparse as sp

## Load and clean some names

conn = MySQLdb.connect(host="127.0.0.1",
                       port=3306,
                       user="markhuberty",
                       passwd="patstat_huberty",
                       db="patstatOct2011",
                       use_unicode=True,
                       charset='utf8'
                       )

conn_cursor = conn.cursor()
conn_cursor.execute("""
SELECT person_name FROM tls206_person LIMIT 10000
""")

person_vec = conn_cursor.fetchall()
conn_cursor.close()
conn.close()

names = [unicode(p[0]) for p in person_vec if len(p[0]) > 1]
del person_vec

all_dicts = [psCleanup.convert_html,
             psCleanup.convert_sgml,
             psCleanup.clean_symbols,
             psCleanup.single_space,
             psCleanup.ampersand,
             psCleanup.us_uk,
             psCleanup.abbreviations
             ]

#Function names below are not exact
N = len(names)
t0 = time.time()
clean_names = [psCleanup.rem_diacritics(n) for n in names]
clean_names = [psCleanup.rem_trail_spaces(n) for n in clean_names]
clean_names = [psCleanup.stdize_case(n) for n in clean_names]
clean_names = psCleanup.master_clean_dicts(clean_names, all_dicts)
t1 = time.time()

### Works out to ~ 0.05s / entry
clean_time = t1 - t0
print clean_time / N
## Note that this was 0.0003 s / name this time around.


## Then distance the cleaned names
t0 = time.time()
ngram_mat = psDisambig.build_incremental_ngram_mat(clean_names, n=2)
t1 = time.time()

## Works out to ~ 0.0005s / entry
ngram_mat_time = t1 - t0
print ngram_mat_time / N

## Define the cosine function
t0 = time.time()
ngram_cosine_mat = psDisambig.cosine_similarity(ngram_mat['tf_matrix'])
ngram_cosine_mat.setdiag([0] * ngram_cosine_mat.shape[0])
t1 = time.time()

cosine_time = t1 - t0
print cosine_time / N


# matches = []
# vals = []
# for row in range(cosine_mat.shape[0]):
#     try:
#         this_row = cosine_mat.getrow(row)
#         row_max = this_row.indices[this_row.data.argmax()] if this_row.nnz else 0
#         row_val = this_row.data.max()
#     except:
#         row_max = None
#         row_val = None
#     matches.append(row_max)
#     vals.append(row_val)
