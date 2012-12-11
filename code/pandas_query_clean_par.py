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
import subprocess
import csv
import psCleanup
import MySQLdb
import time
import numpy
import pandas as pd
import pandas.io.sql as psql
from IPython.parallel import Client
import gc
import itertools as it
from cleanup_dicts import *
from psCleanup import name_address_dict_list, coauth_dict_list, legal_regex
import re

# @dview.parallel(block=True)
# def ipc_clean_wrapper(ipc_code_list):
#      out = psCleanup.ipc_clean_atomic(ipc_code_list)
#      return out

def coauthor_aggfun(col):
     col_short = list(col)[0:9]
     return '**'.join(col_short)

# Set up MySQL connection

db=MySQLdb.connect(host='localhost', port = 3306, user='mimitam',
                   passwd='tam_patstat2011', db = 'patstatOct2011'
                   )

def myquery(query, db_connection, colnames):
     output = psql.frame_query(query, con=db_connection)
     output.columns = colnames
     return output

    
# Years to group patent data by.
 
years = [#'1990',
         '1991',
         '1992',
         '1993', '1994', '1995', '1996', '1997', '1998', '1999',
         '2000', '2001', '2002', '2003', '2004', '2005', '2006',
         '2007', '2008', '2009',
         '2010', '2011']

total_elapsed_time = 0


for year in years:
     # Start the cluster for each run; clears out the memory better
     os.system('ipcluster start -n 2 &')
     time.sleep(60)
     # subprocess.Popen(['ipcluster', 'start', '-n', '2'], stdout=subprocess.PIPE) 
     par_client = Client()
     dview = par_client[:]
     dview.block = True

     # Sync the necessary imports and path settings
     dview.execute('import sys')
     dview.execute('sys.path.append("/home/markhuberty/Documents/psClean/code")')

     with dview.sync_imports():
          import psCleanup

     dview.push({'name_address_dict_list': name_address_dict_list})
     dview.push({'coauth_dict_list': coauth_dict_list})
     dview.push({'legal_regex': legal_regex})

     name_address_regex = [psCleanup.make_regex(d) for d in name_address_dict_list]
     coauth_regex = [psCleanup.make_regex(d) for d in coauth_dict_list]

     dview.push({'name_address_regex': name_address_regex})
     dview.push({'coauth_dict_list': coauth_regex})

     # Set up parallel cleaning wrappers
     @dview.parallel(block=True)
     def name_clean_wrapper(name_list, clean_regex=name_address_regex, legal_regex=legal_regex):
          name_string = psCleanup.decoder(name_list)
          name_string = psCleanup.remove_diacritics(name_string)
          name_string = psCleanup.stdize_case(name_string)
          name_string = psCleanup.master_clean_regex(name_string, clean_regex)
          names_ids = psCleanup.get_legal_ids(name_string, legal_regex)
          return names_ids
     
     @dview.parallel(block=True)
     def address_clean_wrapper(address_list, clean_regex=name_address_regex):
          address_string = psCleanup.decoder(address_list)
          address_string = psCleanup.remove_diacritics(address_string)
          address_string = psCleanup.stdize_case(address_string)
          address_string = psCleanup.master_clean_regex(address_string, clean_regex)
          return address_string

     name_extract = """
     SELECT
        tls207_pers_appln.appln_id, tls206_person.person_id,
        tls206_person.person_name, tls206_person.person_address, tls206_person.person_ctry_code
     FROM tls206_person INNER JOIN tls207_pers_appln ON tls206_person.person_id = tls207_pers_appln.person_id
     INNER JOIN tls201_appln ON tls201_appln.appln_id = tls207_pers_appln.appln_id
     WHERE YEAR(tls201_appln.appln_filing_date) = """+ year #+ """ LIMIT 10000"""

     ipc_extract = """
     SELECT
     tls201_appln.appln_id, GROUP_CONCAT(tls209_appln_ipc.ipc_class_symbol SEPARATOR '**')
     FROM tls201_appln INNER JOIN tls209_appln_ipc ON tls209_appln_ipc.appln_id = tls201_appln.appln_id 
     WHERE YEAR(tls201_appln.appln_filing_date) = """+ year +"""
     GROUP BY tls201_appln.appln_id
     """
    
     date = time.strftime('%c', time.localtime()) 
     print 'Processing ' + year + '.    Started: '+ time.strftime('%c', time.localtime())
    
     name_colnames = ['appln_id', 'person_id', 'person_name', 'person_address',
                      'person_ctry_code'
                      ]
     ipc_colnames = ['appln_id', 'ipc_code']
     
     start_time = time.time()
     name_output = myquery(name_extract, db, name_colnames)
     ipc_output = myquery(ipc_extract, db, ipc_colnames)
     
     print time.strftime('%c', time.localtime())
     print 'Query complete, starting cleaning'
     print 'Number of records returned:'
     print len(name_output), len(ipc_output)
     
     name_clean_time = time.time()
     
     names_ids = name_clean_wrapper.map(list(name_output['person_name']))
     dview.results.clear()
     par_client.results.clear()
     name_output['person_name'], name_output['firm_legal_id'] = it.izip(*names_ids)
     del names_ids
     gc.collect()
     
     name_output['person_address'] = address_clean_wrapper.map(list(name_output['person_address']))
     dview.results.clear()
     par_client.results.clear()
     print time.strftime('%c', time.localtime())
     print 'Names clean'
     # ID the coauthors and join

     coauthor_list = []
     coauth_time = time.time()
     names_grouped = name_output.groupby('appln_id')
     coauthors = names_grouped['person_name'].agg(coauthor_aggfun)
     coauthors.name = 'coauthors'

     name_output.set_index('appln_id', inplace=True)
     name_output = name_output.join(coauthors)
     print (time.time() - coauth_time)

     coauth_clean_time = time.time()
     coauth_clean = []
     for n,c in it.izip(name_output['person_name'], name_output['coauthors']):
          
          coauthors = c.split('**')
          coauthors_clean = [ca for ca in coauthors
                             if ca != n
                             ]
          coauthors = '**'.join(coauthors_clean)
          coauth_clean.append(coauthors)
     name_output['coauthors'] = coauth_clean
     
     print time.time() - coauth_clean_time

     # Then here just join the coauthors to the original db
    
     print time.strftime('%c', time.localtime())
     print 'Coauthors aggregated'
    
     ipc_grouped = ipc_output.groupby('appln_id')
     ipc_cat = ipc_grouped['ipc_code'].agg(coauthor_aggfun)
     name_output = name_output.join(ipc_cat, how='left')
     name_output['ipc_code'] = [psCleanup.ipc_clean_atomic(ipc)
                                if isinstance(ipc, str) else ipc
                                for ipc in name_output['ipc_code']]
     del ipc_output, ipc_cat
     gc.collect()
     # Write out files by country-year
     name_clean_finish = time.time()
     print 'Cleaning time per name + address + ipc code'
     print (name_clean_finish - name_clean_time) / float(len(name_output))

     end_time = time.time()
     elapsed_time = end_time - start_time
     n_records = str(len(name_output))
     print 'Time elapsed for ' + year + ' and ' + n_records + ' records: ' + str(numpy.round(elapsed_time, 0))

     name_output = name_output.reset_index()
     name_output['year'] = year
     grouped_country = name_output.groupby('person_ctry_code')
     
     if year == '1990':
          header_bool=True
     else:
          header_bool=False
          
     for country, group in grouped_country:
          print country
          output_filename = 'cleaned_output_' + country + '.tsv'
          group.to_csv(output_filename, mode='a', sep='\t', header=header_bool)

     del name_output
     del grouped_country
     os.system('ipcluster stop &')
     time.sleep(60)
     gc.collect()
