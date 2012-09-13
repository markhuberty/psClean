import os
import MySQLdb
import csv
import re
import time

os.chdir('/home/markhuberty/Documents/psClean')
conn = open('./data/ipc_green_inventory_tags_8dig.csv')
reader = csv.reader(conn)
ipc_codes = [row[-1] for row in reader]
conn.close()

## Format correctly
ipc_codes = [re.sub(' ', '   ', code) for code in ipc_codes]
ipc_strings = ','.join(['%s'] * len(ipc_codes))

dbconn = MySQLdb.connect(host="127.0.0.1",
                         port=3306,
                         user="markhuberty",
                         passwd="patstat_huberty",
                         db="patstatOct2011",
                         use_unicode=True,
                         charset='utf8'
                         )


start_time = time.time()
conn_cursor = dbconn.cursor()
conn_cursor.execute("""
SELECT appln_id, ipc_class_symbol FROM tls209_appln_ipc WHERE ipc_class_symbol IN (%s)
""" % ipc_strings, tuple(ipc_codes))

ipc_ids = conn_cursor.fetchall()
conn_cursor.close()
dbconn.close()
end_time = time.time()

time_diff = end_time - start_time
print time_diff

fieldnames = ['appln_id', 'ipc_code']
conn_out = open('./data/ipc_grn_class_ids.csv', 'wt')
writer = csv.writer(conn_out)
writer.writerow(fieldnames)
for item in ipc_ids:
    writer.writerow([str(item[0]), item[1]])
conn_out.close()
