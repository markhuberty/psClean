import sys
sys.path.append('/home/markhuberty/psClean/code')
import psDisambig
import MySQLdb

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
SELECT person_name FROM tls206_person LIMIT 5000000
""")

person_vec = conn_cursor.fetchall()
conn_cursor.close()
conn.close()

person_list = [unicode(p[0]) for p in person_vec if len(p) > 1]
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
