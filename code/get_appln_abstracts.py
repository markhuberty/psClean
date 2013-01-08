import MySQLdb
import os
import time
import pandas as pd
import pandas.io.sql as psql

os.chdir('/home/markhuberty/Documents/psClean/data')


df_nl = pd.read_csv('')

appln_ids = df_nl.appln_id.values

in_list = ', '.join(list(map(lambda x: '%s', appln_ids)))

this_query = """SELECT appln_id, appln_abstract FROM tls203
                WHERE appln_id IN (%s)""" % in_list

db=MySQLdb.connect(host='localhost', port = 3306, user='markhuberty',
                   passwd='patstat_huberty', db = 'patstatOct2011'
                   )

nl_abstracts = psql.frame_query(this_query, con=db)
nl_abstracts.columns = ['appln_id', 'appln_abstract']

df_out = merge(df_nl, nl_abstracts, how='inner', on='appln_id')

df_out.to_csv('nl_abstract.csv', index=False)
