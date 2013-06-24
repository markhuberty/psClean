############################
## Author: Mark Huberty, Mimi Tam, and Georg Zachmann
## Date Begun: 13 November 2012
## Purpose: Extract PATSTAT inventor data and do preliminary cleaning
## License: See LICENSE file in root
## Copyright (c) 2012-2013, Authors
## All rights reserved.
############################
from IPython.parallel import Client
from cleanup_dicts import *
from psCleanup import name_address_dict_list, coauth_dict_list, legal_regex
import MySQLdb
import csv
import gc
import itertools as it
import numpy as np
import os
import pandas as pd
import pandas.io.sql as psql
import psCleanup
import re
import sys
import time

"""
Command-line script to extract data from PATSTAT for disambiguation,
and do preliminary cleaning and consolidation. Outputs one file per country,
with one row per PATSTAT person_id.

The script must be invoked with ipython, in order to take advantage of parallism on machines
with > 1 core.

Takes two command line arguments. In order:

1. The destination directory for file output
2. The number of cores to use for parallelism

"""

# @dview.parallel(block=True)
# def ipc_clean_wrapper(ipc_code_list):
#      out = psCleanup.ipc_clean_atomic(ipc_code_list)
#      return out

def coauthor_aggfun(col):
     col_short = list(col)[0:9]
     return '**'.join(col_short)

def myquery(query, db_connection, colnames):
     output = psql.frame_query(query, con=db_connection)
     output.columns = colnames
     return output

# Point the script to the correct output directory
# Assumes the script is invoked as ipython extract_patstat_data.py <output_dir> <cores>
inputs = [i for idx, i in enumerate(sys.argv) if idx > 0]
output_dir = inputs[0]
cores = inputs[1]

# Set up MySQL connection
db=MySQLdb.connect(host='localhost',
                   port=3306,
                   user='',
                   passwd='',
                   db = 'patstatOct2011'
                   )
    
# Years to group patent data by.
years = ['1991', '1992', '1993', '1994', '1995', '1996', '1997',
         '1998', '1999', '2000', '2001', '2002', '2003', '2004',
         '2005', '2006', '2007', '2008', '2009', '2010', '2011'
         ]

total_elapsed_time = 0

# This loop extract PATSTAT data by year, cleans and aggregates it,
# and then writes out the data to country-specific files. 
for year in years:
     # Start the cluster for each run; clears out the memory better
     os.system('ipcluster start -n ' + cores + ' &')
     time.sleep(60)

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

     # Clean names and separate legal IDs if possible
     names_ids = name_clean_wrapper.map(list(name_output['person_name']))
     dview.results.clear()
     par_client.results.clear()
     name_output['person_name'], name_output['firm_legal_id'] = it.izip(*names_ids)
     del names_ids
     gc.collect()

     # Clean addresses
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

     # Clean the coauthors and append back to the original file
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
     print time.strftime('%c', time.localtime())
     print 'Coauthors aggregated'

     # Format the IPC patent codes as a delimited string
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
     print 'Time elapsed for ' + year + ' and ' + \
           n_records + ' records: ' + str(np.round(elapsed_time, 0))

     name_output = name_output.reset_index()
     name_output['year'] = year
     grouped_country = name_output.groupby('person_ctry_code')
     
     if year == '1990':
          header_bool=True
     else:
          header_bool=False

     # Write out; if file exists, then append
     for country, group in grouped_country:
          print country
          output_filename = output_dir + 'cleaned_output_' + country + '.tsv'
          group.to_csv(output_filename, mode='a', sep='\t', header=header_bool)

     # Reset the data and stop the cluster
     del name_output
     del grouped_country
     os.system('ipcluster stop &')
     time.sleep(60)
     gc.collect()
