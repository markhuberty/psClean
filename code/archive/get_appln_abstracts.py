import MySQLdb
import os
import time
import pandas as pd
import pandas.io.sql as psql

os.chdir('/home/markhuberty/Documents/psClean/data')

def field_aggfun(col, delim='**'):
    return delim.join(col)

def field_identity(col):
    return col[0]

df_nl = pd.read_csv('./cleaned_data/cleaned_output_NL.tsv')

appln_ids = df_nl.appln_id.values
in_list = ', '.join(list(map(str, appln_ids)))

db=MySQLdb.connect(host='localhost', port = 3306, user='markhuberty',
                   passwd='patstat_huberty', db = 'patstatOct2011'
                   )

this_query = """SELECT tls203_appln_abstr.appln_id, tls203_appln_abstr.appln_abstract, tls217_appln_ecla.epo_class_symbol FROM tls203_appln_abstr INNER JOIN tls217_appln_ecla ON tls203_appln_abstr.appln_id=tls217_appln_ecla.appln_id WHERE tls203_appln_abstr.appln_id IN (%s) AND tls217_appln_ecla.epo_class_scheme='EC';""" % (in_list) 
nl_abstracts = psql.frame_query(this_query, con=db)

nl_abstracts.columns = ['appln_id', 'appln_abstract', 'ecla_class']
nl_grouped = nl_abstracts[['appln_id', 'ecla_class']].groupby('appln_id')
nl_agg = nl_grouped['ecla_class'].agg(field_aggfun)
nl_abstracts.set_index('appln_id', inplace=True)

nl_out = nl_abstracts[['appln_abstract']].join(nl_agg)
nl_out.to_csv('nl_abstract.csv', index=True)
nl_abstracts.to_csv('nl_abstracts_raw.csv', index=True)
