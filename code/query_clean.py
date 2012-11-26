############################
## Author: Mark Huberty, Mimi Tam, and Georg Zachmann
## Date Begun: 13 November 2012
## Purpose: Module to clean inventor / assignee data in the PATSTAT patent
##          database
## License: BSD Simplified
## Copyright (c) 2012, Authors
## All rights reserved.
##
## Redistribution and use in source and binary forms, with or without
## modification, are permitted provided that the following conditions are met: 
## 
## 1. Redistributions of source code must retain the above copyright notice, this
##    list of conditions and the following disclaimer. 
## 2. Redistributions in binary form must reproduce the above copyright notice,
##    this list of conditions and the following disclaimer in the documentation
##    and/or other materials provided with the distribution. 
## 
## THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
## ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
## WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
## DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR
## ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
## (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
## LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
## ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
## (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
## SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
## 
## The views and conclusions contained in the software and documentation are those
## of the authors and should not be interpreted as representing official policies, 
## either expressed or implied, of the FreeBSD Project.
############################


import os
import csv
import psCleanup
import MySQLdb
import time
import numpy


# Set up MySQL connection

db=MySQLdb.connect(host='localhost', port = 3306, user='mimitam',\
                        passwd='tam_patstat2011', db = 'patstatOct2011')

outpathname = os.getcwd()[:-4]+'output/'

def myquery(query):
    conn_cursor = db.cursor()
    conn_cursor.execute(query)
    output = conn_cursor.fetchall()
    conn_cursor.close()
    
    return output

def tuple_clean(query_output):
    """
    Cleans, formats, outputs the query data.
    Collects summary statistics per country: number of records,
        number of nonblank address lines and average number of coauthors
        and ipc codes per country.
    
    Args:
        query_output: tuple of unformated person_appln tuples
    Returns:
        Files of cleaned person_appln rows written out by country.
        File of summary statistics written out one row per country.
    """

    auth_patent_n = len(query_output)
    addresses_n = 0
    coauths = list()
    ipc = list()


    for record in query_output:

        ## Unpack the tuple
        appln_id, person_id, person_name, person_address, person_ctry_code, \
                  coauthors, ipc_codes = record
        
        ## Separate out the authors and ipcs for cleaning
        coauths_split = record[5].split('**')
        ipc_split = record[6].split('**')

        ## Drop the co-author that is this author
        coauthors = [name for name in coauths_split if name != person_name]

        ## Generate some summary statistics
        addresses_n += len(person_address) > 0
        coauths.append(len(coauths_split))
        ipc.append(len(ipc_split))

        
        appln_id = str(appln_id)
        person_id = str(person_id)

        ## Clean the person name, then break out the
        ## legal identifiers
        raw_name = psCleanup.name_clean([person_name])
        clean_name, firm_legal_ids = psCleanup.get_legal_ids(raw_name)

        clean_coauthors = psCleanup.name_clean(coauthors)
        clean_ipcs = psCleanup.ipc_clean(ipc_split)

        coauthors_final = psCleanup.get_max(coauthors_clean)
        ipc_codes_final = psCleanup.get_max(ipc_codes)
        legal_ids_final = psCleanup.get_max(firm_legal_ids)

        filename = outpathname + record[4]+'_out'
        
        with open(filename, 'a') as tabfile:
            cleanwriter = csv.writer(tabfile, delimiter ='\t')
            cleanwriter.writerow(appln_id,
                                 person_id,
                                 clean_name,
                                 person_address,
                                 legal_ids_final,
                                 person_ctry_code,
                                 coauthors_final,
                                 ipc_codes_final,
                                 year
                                 )

    coauth_mean = numpy.mean(coauths) 
    ipc_mean = numpy.mean(ipc)

    with open(outpathname+'summary_stats', 'a') as csvfile:
        statswriter = csv.writer(csvfile)
        statswriter.writerow([year, auth_patent_n, addresses_n, coauth_mean, ipc_mean])       

    return None

    
# Years to group patent data by.

years = ['1990', '1991', '1992', '1993', '1994', '1995', '1996', '1997', '1998', '1999',
         '2000', '2001', '2002', '2003', '2004', '2005', '2006', '2007', '2008', '2009',
         '2010', '2011']

total_elapsed_time = 0

for year in years:
    dataextract = """
    SELECT
        tls207_pers_appln.appln_id, tls206_person.person_id,
        tls206_person.person_name, tls206_person.person_address, tls206_person.person_ctry_code,
        GROUP_CONCAT(DISTINCT tls206_person.person_name SEPARATOR '**'),
        GROUP_CONCAT(DISTINCT tls209_appln_ipc.ipc_class_symbol SEPARATOR '**')
    FROM tls206_person INNER JOIN tls207_pers_appln ON tls206_person.person_id = tls207_pers_appln.person_id
        INNER JOIN tls201_appln ON tls201_appln.appln_id = tls207_pers_appln.appln_id
        INNER JOIN tls209_appln_ipc ON tls209_appln_ipc.appln_id  = tls207_pers_appln.appln_id 
    WHERE tls201_appln.appln_id = tls207_pers_appln.appln_id
          AND YEAR(tls201_appln.appln_filing_date) = """+ year +"""
    GROUP BY tls207_pers_appln.appln_id ORDER BY NULL
    """


    date = time.strftime('%c', time.localtime()) 
    print 'Processing ' + year + '.    Started: '+ time.strftime('%c', time.localtime())

    time_start = time.time()
    
    query_output = myquery(dataextract)
    final_output = tuple_clean(query_output)

    time_end = time.time()
    elapsed_time = time_end - time_start
    total_elapsed_time += elapsed_time
    
    print 'Time elapsed for ' + year + ': ' + str(numpy.round(elapsed_time, 0))
    print 'Overall elapsed time: ' + str(numpy.round(elapsed_time, 0))
