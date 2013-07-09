import pandas as pd
import re
import sys
import os

patstat_output = './test/'
patstat_files = os.listdir(patstat_output)
patstat_files = [f for f in patstat_files if 'csv' in f]

for idx, f in enumerate(patstat_files):
    patstat_file = pd.read_csv(patstat_output + f)
    country = f[0:2] ## files of form stuff_countrycode.csv
    print country
    amadeus_filename = '/home/markhuberty/Documents/psClean/data/amadeus/input/clean_geocoded_%s.txt' % country.upper()
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

    joint_file = joint_file[['naics', 'ipc_code', 'cluster_id']]
    joint_file.columns = ['naics', 'ipc_codes', 'patstat_cluster']

    global_ipc_codes = [' '.join(ipc.split('**')) if isinstance(ipc, str) else ''
                        for ipc in joint_file.ipc_codes]

    df_temp = pd.DataFrame({'naics': joint_file.naics,
                            'cluster_id': joint_file.patstat_cluster,
                            'ipc_codes': global_ipc_codes}
                           )
    df_temp = df_temp.drop_duplicates()
    df_temp = df_temp.dropna()

    if idx == 0:
        df_all = df_temp
    else:
        df_all = pd.concat([df_all, df_temp], axis=0, ignore_index=True)

df_all.to_csv('naics_8dig_ipc_df.csv', index=False)
    
    
