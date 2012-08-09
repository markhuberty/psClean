import sys
sys.path.append('/home/markhuberty/Documents/psClean/code')
import re
import string
import numpy as np
import scipy.sparse as sp
import csv
import time
from IPython.parallel import Client
import gc

namefile_path = '/mnt/db_master/patstat_raw/dvd1'
namefiles = ['tls206_part0' + str(i) + '_clean.txt' for i in range(1,6)]

## Generate the parallel clients
rc = Client()
dview = rc[:]
dview.block = True

## Sync the necessary imports and path settings
dview.execute('import sys')
dview.execute('sys.path.append("/home/markhuberty/Documents/psClean/code")')

with dview.sync_imports():
    import psCleanup
    import psDisambig


## Define the cleaning dicts and push to each
## client node
all_dicts = [psCleanup.convert_html,
             psCleanup.convert_sgml,
             psCleanup.clean_symbols,
             psCleanup.concatenators,
             psCleanup.single_space,
             psCleanup.ampersand,
             psCleanup.us_uk,
             psCleanup.abbreviations
             ]
regex_dicts = [psCleanup.make_regex(d) for d in all_dicts]
dview.push({'all_dicts': all_dicts})
dview.push({'regex_dicts': regex_dicts})


## Wrap the clean sequence in a useful function that
## can be directly parallelized. T
@dview.parallel(block=True)
def clean_wrapper(name_dict, dict_list=regex_dicts):
    name_string = psCleanup.decoder(name_dict['person_name'])
    out = psCleanup.rem_diacritics(name_string)
    out = psCleanup.stdize_case(out)
    out = psCleanup.master_clean_regex([out], dict_list)
    out = out[0].strip()
    name_dict['person_name'] = psCleanup.encoder(out)
    return(name_dict)



fieldnames = ['person_id',
              'person_ctry_code',
              'doc_std_name_id',
              'person_name',
              'person_address'
              ]
block_size = 50000

## Define a chunker to import N rows and clean
## in parallel
def gen_chunks(reader, chunksize=100):
        """
        Chunk generator. Take a CSV `reader` and yield
        `chunksize` sized slices.
        """
        chunk = []
        for index, line in enumerate(reader):
            if (index % chunksize == 0 and index > 0):
                yield chunk
                del chunk[:]
            chunk.append(line)
        yield chunk


for f in namefiles:
    print f
    total_chunks = 0
    full_path = namefile_path + '/' + f
    full_output_path = full_path + '.namestd'
    output_conn = open(full_output_path, 'wt')
    output_writer = csv.DictWriter(output_conn, fieldnames=fieldnames)
    with open(full_path, 'rt') as namefile:
        reader = csv.DictReader(namefile, fieldnames=fieldnames)
        for process_chunk in gen_chunks(reader, chunksize=block_size):
            t0 = time.time()
            out = clean_wrapper.map(process_chunk)
            output_writer.writerows(out)
            t1 = time.time()
            total_chunks += 1
            del out[:]
            print total_chunks, total_chunks * block_size, (t1 - t0) / block_size
            if total_chunks % 10 == 0 and total_chunks > 0:
                ## Clean out cached objects on the clients
                rc.purge_results(targets=rc.ids)
                dview.results.clear()
                rc.results.clear()
                gc.collect()
    output_conn.close()
