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



for idx, country in enumerate(eu27):
    patstat_file = pd.read_csv()
    amadeus_file = pd.read_csv()

    joint_file = pd.merge(patstat_file,
                          amadeus_file,
                          left_on='Name',
                          right_on='name'
                          how='inner'
                          )

    joint_file = joint_file[['naics', 'Class']]
    joint_file.columns = ['naics', 'ipc_codes']

    global_ipc_codes = [' '.join(ipc.split('**')) for ipc in joint_file.ipc_codes]

    df_temp = pd.DataFram(joint_file.naics, global_ipc_codes)
    df_temp.columns = ['naics', 'ipc_codes']

    if idx == 0:
        df_all = df_temp
    else:
        df_all = pd.concat(df_all, df_temp, axis=0, ignore_index=True)

vectorizer = fe.text.CountVectorizer()
v_fit = vectorizer.fit(global_ipc_codes)
v_mat = vectorizer.transform(global_ipc_codes)

tf_transformer = fe.TfidfTransformer(use_idf=True).fit(v_mat)
v_mat_tfidf = tf_transformer.transform(v_mat)

    
    
