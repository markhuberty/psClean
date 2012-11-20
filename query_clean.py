# 7 November 2012
# Prepare the query output for disambiguation

import os
import csv
import psCleanup
import MySQLdb
import time
import numpy


testquery = ((559L, 31833444L, 'Le D\xc3\xb4me  2-8 Avenue Charles de Gaulle,1653 Luxembourg', 'SMR Patents S.\xc3\xa0.r.l.',\
 'B60R   1/06**B60S   1/02**B60S   1/08**G02B  27/00**G06T   7/00**H05B   3/84'), (1290L, 14677518L, '15 rue Edwa\
rd Steichen,2540 Luxembourg', 'Intrasonics S.\xc3\xa0.r.l.', 'A63H   3/36**G01S   5/30**G10L  19/00**G10L  19/02**G10L  19/14**H03M   7/00**H04H\
  20/31**H04M   1/215**H04M   1/247**H04M   1/253**H04M   1/57**H04N   5/60**H04N   7/08**H04N   7/088'), (1594L, 16565634L, '31, rue N.S. Pierret,L-2335 Luxembourg', 'KALLSTENIUS, Thomas', 'H04B  10/02**H04B  10/08'), (1937L, 408665L, 'Agostini, Giorgio', ' 7, rue\
 de Luxembourg,L-7733, Colmar-Berg', 'Agostini, Giorgio**Corvasce, Filomeno Gennaro**Lechtenboehmer, Annette', 'B60C   1/00**C08K   3/34**C08L  \
21/00'), (2500L, 1354525L, '66, rue de Luxembourg,4221 Esch-sur-Alzette', 'ArcelorMittal Commerc\
ial RPS S.\xc3\xa0 r.l.**HERMES, Aloyse', 'E02D   5/04'), (2826L, 933234L, 'Zone Industrielle,8287 Kehlen', 'Amer-Sil S.A.**LAM\
BERT, Urbain', 'H01M   2/38**H01M   4/68**H01M   4/76**H01M  10/06**H01M  10/12**H01M  10/14**H01M  10/42'), (2928L, 9353225L, '10b, Rue des M\xc3\xa9rovingiens (ZI Bourmicht),8070 Bertrange', 'Flooring Industries Limited, SARL', 'B44C   3/02**B44F   \
9/02'), (3600L, 20412856L,' 65, rue de la Foret,L-7227, Bereldange', 'Lionetti, Robert Edward**Parsons, Anthony Will\
iam', 'G01B   7/26**G01B  11/22'))



# Set up MySQL connection

#db=MySQLdb.connect(host='localhost', port = 3306, user='mimitam', passwd='tam_patstat2011', db = 'patstatOct2011')

def myquery(query):
    start = time.time()
    conn_cursor = db.cursor()
    conn_cursor.execute(query)
    output = conn_cursor.fetchall()
    conn_cursor.close()
    end = time.time()

    runningtime = end - start

    print 'length of query: '+str(runningtime)
    return output

# Get some descriptive stats for each country
def summary_stats(query_output):
    """
    Gets the number of records, number of nonblank address lines and average number of coauthors
    and ipc codes per country.
    Args:
        tuple of unformated person_appln tuples
    Returns:
        summary statistics per country.
    """

    
# Format and write out each authour-patent instance by line.
def tuple_clean(query_output):
    """
    Cleans, formats, outputs the query data.
    Args:
        tuple of unformated person_appln tuples
    Returns:
        cleaned and formated tab delimited string of person_appln ready for writing
    """

    addresses_n = 0
    coauths = list()
    ipc = list()


    for record in query_output:

        addresses_n += len(record[2])> 0
        coauths.append(len(record[3].split('**')))
        ipc.append(len(record[4].split('**')))
                              
        record = list(record)
        record[0] = str(record[0])
        record[1] = str(record[1])
        record[3] = psCleanup.cleanup((record[3].split('**')))
        record[4] = psCleanup.ipc_clean(record[4].split('**'))

        name = psCleanup.get_legal_ids(record[3].pop(0))
        record[3:5] = [psCleanup.get_max(comparison) for comparison in record[3:5]]

        with open(country+'_out', 'a') as tabfile:
            cleanwriter = csv.writer(tabfile, delimiter ='\t')
            cleanwriter.writerow([record[0], record[1], name[0], name[1], record[2], record[3], record[4]])    

    coauth_mean = numpy.mean(coauths)
    ipc_mean = numpy.mean(ipc)

    with open('summary_stats', 'a') as csvfile:
        statswriter = csv.writer(csvfile)
        statswriter.writerow([country, addresses_n, coauth_mean, ipc_mean])       

    return None

    
# Run query and cleanup by country.

countries = ['A ', 'AB']#, 'AC', 'AD', 'AE', 'AF', 'AG', 'AI', 'AJ', 'AL', 'AM', 'AN', 'AO', 'AP', 'AQ', 'AR', 'AS', 'AT', 'AU', 'AW', 'AX', 'AZ', \
#'\xc3\x88']


for country in countries:

    dataextract = """
        EXPLAIN
        SELECT
            tls207_pers_appln.appln_id, tls207_pers_appln.person_id, tls206_person.person_address,
            GROUP_CONCAT(DISTINCT tls206_person.person_name SEPARATOR '**'),GROUP_CONCAT(DISTINCT tls209_appln_ipc.ipc_class_symbol SEPARATOR\
         '**')
        FROM
            tls201_appln, tls206_person,
            tls207_pers_appln JOIN tls209_appln_ipc ON tls207_pers_appln.appln_id = tls209_appln_ipc.appln_id
        WHERE tls207_pers_appln.person_id = tls206_person.person_id AND tls206_person.person_ctry_code = """+country+"""
              AND tls207_pers_appln.appln_id = tls201_appln.appln_id AND YEAR(tls201_appln.appln_filing_date) > 1990
        GROUP BY tls207_pers_appln.appln_id ORDER BY NULL
        """
    
    date = time.strftime('%c', time.localtime()) 
    print 'Processing '+country+'.    Started: '+time.strftime('%c', time.localtime())
    
   # query_output = myquery(dataextract)
    final_output = tuple_clean(testquery)

