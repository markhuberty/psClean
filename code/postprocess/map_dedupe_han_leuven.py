"""
Command-line script for matching the fung output to the HAN
and Leuven data and writing out the map.

Assumes the following argument order:
(1) the formatted dedupe output file (csv)
(2) the han file 
(3) the leuven file

Assumes the following fields in each file:
(dedupe) person_id, unique_id(s)
(han) han_id, person_id, han_name
(leuven) person_id, hrm_l2_id, hrm_level
"""

import MySQLdb
import os
import pandas as pd
import pandas.io.sql as psql
import string
import sys


## Get the filenames off the command line

db=MySQLdb.connect(host='localhost',
                   port = 3306,
                   user='markhuberty',
                   passwd='patstat_huberty',
                   db = 'patstatOct2011'
                   )

sys.stdout = os.fdopen(sys.stdout.fileno(), 'wb', 0)


## Command line should be dedupe file, country, output_dir
inputs = [i for idx, i in enumerate(sys.argv) if idx > 0]
dedupe_output_file = inputs[0]
country = inputs[1]
output_root = inputs[2]

## Generate and query the data
han_query = """SELECT HAN_ID, OCT11_Person_id, Person_name_clean FROM han_person WHERE Person_ctry_code='""" + country.upper() + """'"""
leuven_query = """SELECT person_id, hrm_level, hrm_l2_id, person_name FROM leuven_name WHERE person_ctry_code='""" + country.upper() + """'"""

han_data = psql.frame_query(han_query, con=db)
leuven_data = psql.frame_query(leuven_query, con=db)

## Load the files
dedupe_output = pd.read_csv(dedupe_output_file)

print dedupe_output.shape
print han_data.shape
print leuven_data.shape

dedupe_colnames = ['Person']
for c in dedupe_output.columns:
    if 'cluster' in c:
        dedupe_colnames.append(c)

leuven_data =  leuven_data[['person_id',
                            'person_name',
                            'hrm_l2_id',
                            'hrm_level'
                            ]
                           ]
leuven_data.columns = ['person_id',
                       'person_name',
                       'leuven_id',
                       'leuven_ld_level'
                       ]



han_data = han_data[['HAN_ID','OCT11_Person_id','Person_name_clean']]
han_data.columns = ['han_id',
                    'person_id',
                    'han_name'
                    ]


leuven_map = pd.merge( leuven_data, dedupe_output[dedupe_colnames], left_on = 'person_id', right_on = 'Person', how='outer')
han_map = pd.merge( han_data, dedupe_output[dedupe_colnames], left_on = 'person_id', right_on = 'Person', how='outer')

leuven_output_name = output_root + 'dedupe_leuven_map.csv' 
han_output_name = output_root + 'dedupe_han_map.csv'

leuven_map.to_csv(leuven_output_name, index=False)
han_map.to_csv(han_output_name, index=False)
