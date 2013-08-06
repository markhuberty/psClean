import MySQLdb
import numpy as np
import os
import pandas as pd
import pandas.io.sql as psql
import re
import sys

patstat_output = '/home/markhuberty/Documents/psClean/data/dedupe_script_output/'
patstat_files = os.listdir(patstat_output)
patstat_files = [f for f in patstat_files if 'csv' in f]

def consolidate_unique(x):
    """
    Returns the first value in the series
    """
    return x.values[0]

def consolidate_set(x, delim='**', maxlen=100):
    """
    Consolidates all multi-valued strings in x
    into a unique set of maximum length maxlen.

    Returns a multivalued string separated by delim
    """
    vals = [v.split(delim) for v in x.values if isinstance(v, str)]
    val_set = [v for vset in vals for v in vset]
    val_set = list(set(val_set))
    if len(val_set) > 0:
        if len(val_set) > maxlen:
            rand_idx = random.sample(range(len(val_set)), maxlen)
            val_set = [val_set[idx] for idx in rand_idx]
        out = delim.join(val_set)
    else:
        out = ''
    return out

def ipc_consolidate(s):
    out = consolidate_set(s, maxlen=1000)
    return out

agg_dict = {'Name': consolidate_unique,
            'Class': ipc_consolidate
            }

amadeus_query = """SELECT company_name, naics_2007, new_naics
                   FROM amadeus_parent_child
                   WHERE country='%s'"""

for idx, f in enumerate(patstat_files):
    patstat_file = pd.read_csv(patstat_output + f)

    g = patstat_file[['Name', 'Class', 'cluster_id']].groupby('cluster_id')
    patstat_c = g.agg(agg_dict)
    patstat_c.reset_index(inplace=True)
    country = f[-6:-4] ## files of form stuff_countrycode.csv
    print country

    db=MySQLdb.connect(host='localhost',
                   port = 3306,
                   user='markhuberty',
                   passwd='patstat_huberty',
                   db = 'patstatOct2011'
                   )
    amadeus_file = psql.frame_query(amadeus_query % country, con=db)

    amadeus_file['naics_c'] = [n if np.isnan(m) else m
                               for n, m in zip(amadeus_file.naics_2007, amadeus_file.new_naics)
                               ]

    if amadeus_file.shape[0]==0:
        continue
    
    joint_file = pd.merge(patstat_c,
                          amadeus_file,
                          left_on='Name',
                          right_on='company_name',
                          how='inner'
                          )
    joint_file = joint_file[['company_name', 'naics_c', 'Class', 'cluster_id']]
    joint_file.columns = ['company_name', 'naics', 'ipc_codes', 'patstat_cluster']
    joint_file['country'] = country

    global_ipc_codes = [' '.join(ipc.split('**')) if isinstance(ipc, str) else ''
                        for ipc in joint_file.ipc_codes]

    df_temp = pd.DataFrame({'company_name': joint_file.company_name,
                            'naics': joint_file.naics,
                            'cluster_id': joint_file.patstat_cluster,
                            'ipc_codes': global_ipc_codes,
                            'country': joint_file.country}
                           )


    if idx == 0:
        df_all = df_temp
    else:
        df_all = pd.concat([df_all, df_temp], axis=0, ignore_index=True)

df_all.to_csv('naics_ipc_df_new.csv', index=False)
    
    
