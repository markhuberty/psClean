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
                   user='markhuberty',
                   passwd='huberty_patstat',
                   db = 'patstatOct2011'
                   )

id_query = """
SELECT
   appln_auth, COUNT(appln_abstract), COUNT(appln_id) FROM tls201_appln INNER JOIN tls203_appln_abstr ON appln_id GROUP BY country

"""

appln_counts = psql.frame_query(id_query, con=db)
