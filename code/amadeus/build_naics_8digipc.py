import pandas as pd
import re
import sys
import os

patstat_output = './test/'
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

for idx, f in enumerate(patstat_files):
    patstat_file = pd.read_csv(patstat_output + f)
    country = f[-6:-4] ## files of form stuff_countrycode.csv
    print country
    amadeus_filename = '/home/markhuberty/Documents/psClean/data/amadeus/clean_geocoded_%s.txt' % country.upper()
    try:
        amadeus_file = pd.read_csv(amadeus_filename, sep='\t')
    except:
        print 'no amadeus file found for %s' % country
        continue

    joint_file = pd.merge(patstat_file,
                          amadeus_file,
                          left_on='Name',
                          right_on='company_name',
                          how='inner'
                          )

    joint_file = joint_file[['naics', 'Class', 'cluster_id']]
    joint_file.columns = ['naics', 'ipc_codes', 'patstat_cluster']

    global_ipc_codes = [' '.join(ipc.split('**')) if isinstance(ipc, str) else ''
                        for ipc in joint_file.ipc_codes]

    df_temp = pd.DataFrame({'naics': joint_file.naics,
                            'cluster_id': joint_file.patstat_cluster,
                            'ipc_codes': global_ipc_codes}
                           )


    if idx == 0:
        df_all = df_temp
    else:
        df_all = pd.concat([df_all, df_temp], axis=0, ignore_index=True)

df_all.to_csv('naics_ipc_df.csv', index=False)
    
    
