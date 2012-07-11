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



## Define some blocking functions

def block_by_2_ngrams(name_string, ngram_length=2):
    block_dict = {}
    for name in name_string:
        these_ngrams = set([''.join(name[j] for j in range(i, i + n))
                            for i in range((len(name) - n))]
                           )
        ngram_combos = get_combinatorial_pairs(these_ngrams)
        for ngram in ngram_combos:
            if ngram in block_dict:
                block_dict[ngram].append(name)
            else:
                block_dict[ngram] = [name]
    return block_dict


def get_combinatorial_pairs(ngram_list):
    pairs = []
    for i, n in enumerate(ngram_list):
        for j in range(i + 1, len(ngram_list)):
            pair = tuple(sorted([n, ngram_list[j]]))
            if pair not in pairs:
                pairs.append(pair)
    return(pairs)


## Block and count

ngram_blocks = block_by_2_ngrams(clean_names, ngram_length=2)

counts = [len(v) for k, v in ngram_blocks]
