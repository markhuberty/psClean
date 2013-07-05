import pandas as pd
import MySQLdb
import pandas.io.sql as psql
import sys

inputs = [i for idx, i in enumerate(sys.argv) if idx > 0]
username = inputs[0]
pword = inputs[1]

# Set up MySQL connection
db=MySQLdb.connect(host='localhost',
                   port=3306,
                   user=username,
                   passwd=pword,
                   db = 'patstatOct2011'
                   )

id_query = """
SELECT
   appln_auth, COUNT(appln_abstract), COUNT(tls201_appln.appln_id)
   FROM tls201_appln INNER JOIN tls203_appln_abstr
   ON tls201_appln.appln_id=tls203_appln_abstr.appln_id
   WHERE tls203_appln_abstr.appln_abstract IS NOT NULL AND tls203_appln_abstr.appln_abstract!=''
   GROUP BY appln_auth

"""

appln_counts = psql.frame_query(id_query, con=db)
appln_counts.to_csv('patstat_abstract_counts_bycountry.csv', index=False)
