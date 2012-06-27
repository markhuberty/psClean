import sys
sys.path.append('/home/markhuberty/psClean/code')
import psDisambig
import MySQLdb
import re
import time
import string
## Demonstration code for ngram matrix generation
sample_strings = ['this is a string',
                  'my dog has fleas',
                  'we should really use actual data'
                  ]

test_unigram_mat = psDisambig.build_ngram_mat(sample_strings, n=1)
test_bigram_mat = psDisambig.build_ngram_mat(sample_strings, n=2)

print test_unigram_mat['tf_matrix'].todense()
print test_bigram_mat['tf_matrix'].todense()

test_incr_unigram_mat = psDisambig.build_incremental_ngram_mat(sample_strings, n=1)

## Check against actual data
## Note that this doesn't currently do any up-front cleanup or elegant
## unicode handling
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

person_list = [unicode(p[0]) for p in person_vec if len(p[0]) > 1]
del person_vec

## Dump punctuation and excess whitespace
regex = re.compile('[%s]' % re.escape(string.punctuation))
person_list = [regex.sub('', p) for p in person_list]
person_list = [p.strip() for p in person_list if len(p) > 8]
person_list = [p.lower() for p in person_list]

t0 = time.time()
mat = psDisambig.build_incremental_ngram_mat(person_list[0:100000], n=1)
t1 = time.time()

## Takes about 0.0002 s / string, or ~2 hours for 40m names
print t1 - t0

## Also test a few aspects of phontetic matching
import fuzzy
decode_person = [p.encode('ascii', 'ignore') for p in person_list]
decode_person = [d.strip() for d in decode_person if len(d.strip()) > 0]
dmeta = fuzzy.DMetaphone(6)

t0 = time.time()
person_meta_forward = [dmeta(d) for d in decode_person[0:100000]]
person_meta_reverse = [dmeta(d[::-1]) for d in decode_person[0:100000]]
t1 = time.time()

print t1 - t0

## Generate some synthetic keys based on both the
## primary and secondary forward and reverse hashes
meta_primary = []
meta_secondary = []
for i in range(len(person_meta_forward)):
    prim = person_meta_forward[i][0] + '-' + person_meta_reverse[i][0]
    # if person_meta_forward[i][1]:
    #     prim = prim + '-' + person_meta_forward[i][1]
    # if person_meta_reverse[i][1]:
    #     prim = prim + '-' + person_meta_reverse[i][1]
    meta_primary.append(prim)

test = zip(decode_person, meta_primary)#[p[0] for p in person_meta_forward])

import itertools
from operator import itemgetter

test = sorted(test, key=itemgetter(1))
name_matches = []
for k, g in itertools.groupby(test, lambda x:x[1]):
    name_matches.append(list(g))


