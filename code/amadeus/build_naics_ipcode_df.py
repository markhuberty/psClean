import pandas as pd
import re
import sys
import os

# Feature extraction logic
import sklearn.cross_validation as cv
import sklearn.metrics as metrics
from sklearn.naive_bayes import MultinomialNB
from sklearn import feature_extraction as fe

citl_file_root = '../../data/citl_data/geocoded/citl_geocoded_%s.csv'
patent_file_root = '../dedupe/%s_weighted/patstat_output.csv'


eu27 = ['at',
        'bg',
        'be',
        'it',
        'gb',
        'fr',
        'de',
        'sk',
        'se',
        'pt',
        'pl',
        'hu',
        'ie',
        'ee',
        'es',
        'cy',
        'cz',
        'nl',
        'si',
        'ro',
        'dk',
        'lt',
        'lu',
        'lv',
        'mt',
        'fi',
        'el',
        ]

patstat_output = '/home/markhuberty/Documents/psClean/data/dedupe_script_output/'
patstat_files = os.listdir(patstat_output)
patstat_files = [f for f in patstat_files if 'csv' in f]


for idx, f in enumerate(patstat_files):
    patstat_file = pd.read_csv(f)
    country = f[-6:-5] ## files of form stuff_countrycode.csv
    amadeus_filename = '~/Downloads/amadeus/geocoded/clean_geocoded_%s.txt' % country.upper()
    amadeus_file = pd.read_csv(amadeus_filename)

    joint_file = pd.merge(patstat_file,
                          amadeus_file,
                          left_on='Name',
                          right_on='company_name'
                          how='inner'
                          )

    joint_file = joint_file[['naics', 'Class', 'cluster_id']]
    joint_file.columns = ['naics', 'ipc_codes', 'patstat_cluster']

    global_ipc_codes = [' '.join(ipc.split('**')) for ipc in joint_file.ipc_codes]

    df_temp = pd.DataFram(joint_file.naics, joint_file.patstat_cluster, global_ipc_codes)
    df_temp.columns = ['naics', 'ipc_codes']

    if idx == 0:
        df_all = df_temp
    else:
        df_all = pd.concat(df_all, df_temp, axis=0, ignore_index=True)

df_all.to_csv('naics_ipc_df.csv', index=False)
    
    
