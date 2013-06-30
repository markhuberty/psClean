import MySQLdb
import pandas as pd
import pandas.io.sql as psql
import numpy as np

db=MySQLdb.connect(host='localhost', port = 3306, user='markhuberty',
                   passwd='patstat_huberty', db = 'patstatOct2011'
                   )


address_query ="""
SELECT person_ctry_code, COUNT(person_id), SUM(case when person_address!="" THEN 1 ELSE 0 END),
SUM(case when person_name!="" THEN 1 ELSE 0 END)
from tls206_person GROUP BY person_ctry_code;"""

patstat_ct = psql.frame_query(address_query, con=db)
patstat_ct.columns = ['country', 'raw_pid_count', 'raw_address_ct', 'raw_name_ct']

address_ratio = patstat_ct.raw_address_ct.astype(float) / patstat_ct.raw_pid_count
name_ratio = patstat_ct.raw_name_ct.astype(float) / patstat_ct.raw_pid_count

patstat_ct['address_ratio'] = address_ratio
patstat_ct['name_ratio'] = name_ratio

patstat_ct['country'] = [c.lower() for c in patstat_ct.country]

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
        'gr'
        ]

#/home/markhuberty/Documents/psClean/data/dedupe_input/person_records
data_dir = '/home/markhuberty/Documents/psClean/data/dedupe_input/person_records/'
file_root = 'dedupe_input_%s.csv'

countries = []
names = []
addrs = []
coauthors = []
ipcs = []
records = []

for country in eu27:
    filename = file_root % country
    filepath = data_dir + filename
    try:
        df = pd.read_csv(filepath)
    except:
        continue
    records.append(df.shape[0])
    names.append(len(df.Name.dropna()))
    addrs.append(np.sum(~np.isnan(df.Lat)))
    coauthors.append(len(df.Coauthor.dropna()))
    ipcs.append(len(df.Class.dropna()))
    countries.append(country)
    

df_input_stats = pd.DataFrame({'country': countries,
                               'dedupe_name_ct': names,
                               'dedupe_addr_ct': addrs,
                               'dedupe_coauthor_ct': coauthors,
                               'dedupe_ipc_ct': ipcs,
                               'dedupe_record_ct': records}
                              )

# Scale by the total record count
df_input_scaled = df_input_stats.copy(deep=True)
df_input_scaled.set_index('country', inplace=True)
for column in df_input_scaled.columns:
    df_input_scaled[column] = df_input_scaled[column] / df_input_scaled.dedupe_record_ct.astype(float)


out = pd.merge(df_input_scaled, patstat_ct,
               left_index=True,
               right_on='country',
               how='inner'
               )

out.to_csv('../data/patstat_dedupe_summary_statistics_eu27.csv')
