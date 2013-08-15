import MySQLdb
import pandas as pd
import pandas.io.sql as psql

db=MySQLdb.connect(host='localhost',
                   port=3306,
                   user='markhuberty',
                   passwd='patstat_huberty',
                   db = 'patstatOct2011'
                   )

query = """
        SELECT dedupe_id, appln_id FROM
        dedupe_patstat d INNER JOIN tls207_pers_appln t
        WHERE d.person_id=t.person_id
        AND d.person_ctry_code='NL'
"""

test_query = psql.frame_query(query, db)

test = pd.merge(test_query,
                test_query,
                left_on='appln_id',
                right_on='appln_id',
                how='inner'
                )
test = test[test.dedupe_id_x != test.dedupe_id_y]

g = test.groupby(['dedupe_id_x', 'dedupe_id_y'])

dedupe_weights = g.size()


weighted_edges = {}
for idx, val in dedupe_weights.iteritems():
    d1 = idx[0]
    d2 = idx[1]
    if (d1, d2) in weighted_edges:
        weighted_edges[(d1, d2)] += val
    elif (d2, d1) in weighted_edges:
        weighted_edges[(d2, d1)] += val
    else:
        weighted_edges[(d1, d2)] = val

import csv
conn = open('nl_coauthor_edgelist.csv', 'wt')
with open('nl_coauthor_edgelist.csv', 'wt') as conn:
    writer = csv.writer(conn)
    writer.writerow(['n1', 'n2', 'weight'])
    for k, v in weighted_edges.iteritems():
        writer.writerow([k[0], k[1], v])
    
