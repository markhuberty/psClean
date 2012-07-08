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
SELECT person_name FROM tls206_person LIMIT 1000000
""")

person_vec = conn_cursor.fetchall()
conn_cursor.close()
conn.close()

names = [unicode(p[0]) for p in person_vec if len(p[0]) > 1]
del person_vec

all_dicts = [psCleanup.convert_html,
             psCleanup.convert_sgml,
             psCleanup.concatenators,
             psCleanup.single_space,
             psCleanup.ampersand,
             psCleanup.us_uk,
             psCleanup.abbreviations
             ]

def translate_non_alphanumerics(to_translate, translate_to=u' '):
    not_letters_or_digits = unicode(string.punctuation)
    translate_table = dict((ord(char), translate_to)
                           for char in not_letters_or_digits)
    return to_translate.translate(translate_table)

def strip_punc(s):
    s_out = s.translate(string.maketrans("",""), string.punctuation)
    return s_out
#Function names below are not exact
N = len(names)
t0 = time.time()
clean_names = [psCleanup.rem_diacritics(n) for n in names]
clean_names = [psCleanup.rem_trail_spaces(n) for n in clean_names]
clean_names = [psCleanup.stdize_case(n) for n in clean_names]
clean_names = [translate_non_alphanumerics(n) for n in clean_names]
clean_names = psCleanup.master_clean_dicts(clean_names, all_dicts)
clean_names = [n.strip() for n in clean_names]
t1 = time.time()

### Works out to ~ 0.05s / entry
clean_time = t1 - t0
print clean_time / N

## Then pre-cluster by the leading 3 characters of the name
t0 = time.time()
leading_ngram_dict = psDisambig.build_leading_ngram_dict(clean_names, leading_n=3)
t1 = time.time()

leading_ngram_time = t1 - t0
print leading_ngram_time / N

## Then do the disambig on each list:
out = {}
t0 = time.time()
n_gram = 2
for k, v in leading_ngram_dict.iteritems():
    #print k
    if len(k) > 1 and len(v) > n_gram:
        mat = psDisambig.build_incremental_ngram_mat(v,
                                                     n=n_gram
                                                     )
        if mat['tf_matrix'] is not None:
            out[k] = psDisambig.cosine_similarity_match(mat['tf_matrix'])
        else:
            out[k] = None
        
t1 = time.time()

dict_match_time = t1 - t0
print dict_match_time / N
