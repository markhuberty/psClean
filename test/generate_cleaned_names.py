import sys
sys.path.append('/home/markhuberty/Documents/psClean/code')
import psCleanup
import psDisambig
import MySQLdb
import re
import time
import string
import numpy as np
import scipy.sparse as sp

## Define the cleaning dicts
all_dicts = [psCleanup.convert_html,
             psCleanup.convert_sgml,
             psCleanup.concatenators,
             psCleanup.single_space,
             psCleanup.ampersand,
             psCleanup.us_uk,
             psCleanup.abbreviations
             ]

## Wrap the clean sequence in a useful function
def clean_wrapper(name_string, dict_list):
    out = psCleanup.rem_diacritics(name_string)
    out = psCleanup.rem_trail_spaces_out)
    out = psCleanup.stdize_case(out)
    out = psCleanup.master_clean_dicts([name], all_dicts)
    out = out[0].strip()
    return(out)

## Load the sql query as an iterator to save on memory
conn = MySQLdb.connect(host="127.0.0.1",
                       port=3306,
                       user="markhuberty",
                       passwd="patstat_huberty",
                       db="patstatOct2011",
                       use_unicode=True,
                       charset='utf8',
                       cursorclass = MySQLdb.cursors.SSCursor
                       )

conn_cursor = conn.cursor()
conn_cursor.execute("""
SELECT person_id, person_name FROM tls206_person
""")

## Open the connection and write each id + name combination to
## the file as it's cleaned.
output_conn = open('', 'wt')
writer = csv.writer(conn)
for row in conn_cursor:
    person_id, person_name = row
    if len(person_name) > 1:
        person_name = unicode(person_name)
    else:
        continue
    clean_person_name = clean_wrapper(person_name, all_dicts)
    clean_person_name = psCleanup.encoder(clean_person_name
    writer.writerow([person_id, clean_person_name])
output_conn.close()

conn_cursor.close()
conn.close()
