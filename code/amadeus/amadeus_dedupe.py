#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
This code documents a flexible script for reading in PATSTAT data and
disambiguating it. It assumes data of this form:

Name:Coauthor:Class:Lat:Lng

The logic in patent_util is used to read in the data frame from a csv
file and process it into a form useful for the dedupe library. If you wish to
use more or fewer fields in disambiguation, you need to edit
readDataFrame in the patent_util.py file.

The output will be a CSV with our clustered results.

Input files are assumed to be of the form dedupe_input_<country_code>.csv
Output files are written to patstat_output_<date>_<country_code>.csv

The script is invoked from the command line as:

python patstat_dedupe.py <country_code> <input_dir> <output_dir>

For details on how the dedupe algorithm works, see
https://github.com/open-city/dedupe
"""

import collections
import csv
import datetime
import logging
import math
import numpy as np
import optparse
import os
import pandas as pd
import re
import sys
import time
import ipcSectorCompare



sys.path.append('../dedupe')
import AsciiDammit
import patent_util

# Finally load dedupe 
import dedupe

def dbase_diff(s1, s2):
    if s1 == s2:
        return 0
    return 1



def readDataFrame(df, set_delim='**'):
    """
    Read in our data from a pandas DataFrame as an in-memory database.
    Reformat the data into a dictionary of records,
    where the key is a unique record ID and each value is a 
    [frozendict](http://code.activestate.com/recipes/414283-frozen-dictionaries/) 
    (hashable dictionary) of the row fields.

    Remap columns for the following cases:
    - Lat and Long are mapped into a single LatLong tuple
    - Class and Coauthor are stored as delimited strings but mapped into sets

    **Currently, dedupe depends upon records' unique ids being integers
    with no integers skipped. The smallest valued unique id must be 0 or
    1. Expect this requirement will likely be relaxed in the future.**
    """

    data_d = {}

    for idx, dfrow in df.iterrows():
        # print type(dfrow)
        row_out = {}
        row_type = dfrow['dbase']
        if row_type == 'patstat':
            ipc_codes = dfrow['ipc_sector'].split(set_delim)
            ipc_codes = [i for i in ipc_codes if len(i.strip()) > 0]
        else:
            ipc_codes = str(dfrow['ipc_sector']).split('.')[0]

        if isinstance(dfrow['name'], str):
            name = dfrow['name']
        else:
            name = ''
        # row_out['Coauthor_Count'] = len(coauthors)
        # row_out['Class_Count'] = len(classes)
        row_out['LatLong'] = (float(dfrow['lat']), float(dfrow['lng']))
        row_out['name'] = name
        row_out['ipc_sector'] = ipc_codes
        row_out['dbase'] = row_type
        row_tuple = [(k, v) for (k, v) in row_out.items()]
        data_d[idx] = dedupe.core.frozendict(row_tuple)
            
    return data_d

# ## Logging
# Dedupe uses Python logging to show or suppress verbose output. Added
# for convenience.  To enable verbose logging, run `python
# examples/csv_example/csv_example.py -v`

optp = optparse.OptionParser()
optp.add_option('-v', '--verbose', dest='verbose', action='count',
                help='Increase verbosity (specify multiple times for more)'
                )
(opts, args) = optp.parse_args()
log_level = logging.WARNING 
if opts.verbose == 1:
    log_level = logging.INFO
elif opts.verbose >= 2:
    log_level = logging.DEBUG
logging.basicConfig(level=log_level)


# Inputs
# Takes 4 command-line inputs:
# 1. The country to be disambiguated
# 2. The directory containing the input file for disambiguation
# 3. The directory containing the person_id:patent mapping
# 4. The precision-recall weight; larger numbers put greater weight on recall; values
#    usually range from 0.75-3

inputs = [i for idx, i in enumerate(sys.argv) if idx > 0]
country = inputs[0]
input_file_dir = inputs[1]
output_file_dir = inputs[2]
recall_weight = float(inputs[3])

# Set the input / output and training files

this_date = datetime.datetime.now().strftime('%Y-%m-%d')
input_file = input_file_dir + '/' + 'patstat_amadeus_input_' + country + '.csv'
output_file = output_file_dir + '/' + 'amadeus_output_' + this_date + '_' + country + '.csv'
settings_file = 'amadeus_settings_' + this_date + '_' + country + '.json'
training_file = 'amadeus_training_' + this_date + '_' + country + '.json'

print input_file
print output_file
print settings_file
print training_file
print recall_weight

## Set the constants for blocking
ppc=0.001
dupes=5

# Import the data
print 'importing data ...'
input_df = pd.read_csv(input_file)
input_df.ipc_sector.fillna('', inplace=True)
input_df.lat.fillna('0.0', inplace=True)
input_df.lng.fillna('0.0', inplace=True)
input_df.name.fillna('', inplace=True)

# Read the data into a format dedupe can use
data_d = readDataFrame(input_df)

# Build the comparators for ipc/sector
# This loads in a pre-matched set of names and builds from there
naics_ipc = pd.read_csv('naics_ipc_df.csv')
naics_ipc = naics_ipc[['company_name', 'naics', 'ipc_codes']]
naics_ipc = naics_ipc.dropna()
naics_ipc.naics = naics_ipc.naics.astype(int).astype(str)

naics_ipc['naics_2dig'] = [str(int(n))[:2] for n in naics_ipc.naics]

ipc_3dig = []
for ipc_str in naics_ipc.ipc_codes:
        i_split = ipc_str.split(' ')
        i_split = [i[:3] for i in i_split if i != '' and i != ' ']
        ipc_3dig.append(i_split)

ipc_sector_comparator = ipcSectorCompare.ipcSectorCompare(naics_ipc.naics_2dig,
                                                          ipc_3dig,
                                                          ipcSectorCompare.compute_bayes_probs
                                                          )

# Training
if os.path.exists(settings_file):
    print 'reading from', settings_file
    deduper = dedupe.Dedupe(settings_file)

else:
    # To train dedupe, we feed it a random sample of records.
    data_sample = dedupe.dataSample(data_d, 3 * input_df.shape[0])
    # Define the fields dedupe will pay attention to
    fields = {'name': {'type': 'String', 'Has Missing':True},
              'LatLong': {'type': 'LatLong', 'Has Missing':True},
              'ipc_sector': {'type': 'Custom', 'comparator':ipc_sector_comparator},
              'dbase': {'type':'Custom', 'comparator': dbase_diff}
              }

    # Create a new deduper object and pass our data model to it.
    deduper = dedupe.Dedupe(fields)

    # If we have training data saved from a previous run of dedupe,
    # look for it an load it in.
    # __Note:__ if you want to train from scratch, delete the training_file
    # The json file is of the form:
    # {0: [[{field:val dict of record 1}, {field:val dict of record 2}], ...(more nonmatch pairs)]
    #  1: [[{field:val dict of record 1}, {field_val dict of record 2}], ...(more match pairs)]
    # }
    if os.path.exists(training_file):
        print 'reading labeled examples from ', training_file
        deduper.train(data_sample, training_file)

    # ## Active learning

    # Starts the training loop. Dedupe will find the next pair of records
    # it is least certain about and ask you to label them as duplicates
    # or not.

    # use 'y', 'n' and 'u' keys to flag duplicates
    # press 'f' when you are finished
    print 'starting active labeling...'
    deduper.train(data_sample, dedupe.training.consoleLabel)

    # When finished, save our training away to disk
    deduper.writeTraining(training_file)

# Blocking
deduper.blocker_types.update({'Custom': (dedupe.predicates.wholeSetPredicate,
                                         dedupe.predicates.commonSetElementPredicate),
                              'LatLong' : (dedupe.predicates.latLongGridPredicate,)
                              }
                             )
time_start = time.time()
print 'blocking...'

# Initialize the blocker
blocker, ppc_final, ucd_final = patent_util.blockingSettingsWrapper(ppc,
                                                                    dupes,
                                                                    deduper
                                                                    )

# Occassionally the blocker fails to find useful values. If so,
# print the final values and exit.
if not blocker:
    print 'No valid blocking settings found'
    print 'Starting ppc value: %s' % ppc
    print 'Starting uncovered_dupes value: %s' % dupes
    print 'Ending ppc value: %s' % ppc_final
    print 'Ending uncovered_dupes value: %s' % ucd_final
    print 'Exiting'
    sys.exit()

time_block_weights = time.time()
print 'Learned blocking weights in', time_block_weights - time_start, 'seconds'

# Save weights and predicates to disk.
# If the settings file exists, we will skip all the training and learning
deduper.writeSettings(settings_file)

# Generate the tfidf canopy
## NOTE: new version of blockData does tfidf implicitly
# print 'generating tfidf index'
# full_data = ((k, data_d[k]) for k in data_d)
# blocker.tfIdfBlocks(full_data)
# del full_data

# Load all the original data in to memory and place
# them in to blocks. Return only the block_id: unique_id keys
#blocking_map = patent_util.return_block_map(data_d, blocker)

# Note this is now just a tuple of blocks, each of which is a
# recordid: record dict

blocked_data = dedupe.blockData(data_d, blocker)
#keys_to_block = [k for k in blocking_map if len(blocking_map[k]) > 1]
print '# Blocks to be clustered: %s' % len(blocked_data)

# Save the weights and predicates
time_block = time.time()
print 'Blocking rules learned in', time_block - time_block_weights, 'seconds'
print 'Writing out settings'
deduper.writeSettings(settings_file)

# ## Clustering

# Find the threshold that will maximize a weighted average of our precision and recall. 
# When we set the recall weight to 1, we are trying to balance recall and precision
#
# If we had more data, we would not pass in all the blocked data into
# this function but a representative sample.

threshold_data = patent_util.return_threshold_data(blocked_data, 10000)

print 'Computing threshold'
threshold = deduper.goodThreshold(threshold_data, recall_weight=recall_weight)
del threshold_data

# `duplicateClusters` will return sets of record IDs that dedupe
# believes are all referring to the same entity.



print 'clustering...'
# Loop over each block separately and dedupe

clustered_dupes = deduper.duplicateClusters(blocked_data,
                                            threshold
                                            ) 

print '# duplicate sets', len(clustered_dupes)

# Extract the new cluster membership
max_cluster_id = 0
cluster_membership = collections.defaultdict(lambda : 'x')
for (cluster_id, cluster) in enumerate(clustered_dupes):
    for record_id in cluster:
        cluster_membership[record_id] = cluster_id
        if max_cluster_id <= cluster_id:
            max_cluster_id  = cluster_id + 1

# Then write it into the data frame as a sequential index for later use
# Here the cluster ID is either the dedupe ID (if a PATSTAT person belonged
# to a block of 2 or more potential matches) or an integer ID placeholder.

cluster_index = []
clustered_cluster_map = {}
excluded_cluster_map = {}
for df_idx in input_df.index:
    if df_idx in cluster_membership:
        orig_cluster = cluster_membership[df_idx]
        if orig_cluster in clustered_cluster_map:
            cluster_index.append(clustered_cluster_map[orig_cluster])
        else:
            clustered_cluster_map[orig_cluster] = max_cluster_id
            cluster_index.append(max_cluster_id) #cluster_counter)
            max_cluster_id += 1
            # print cluster_counter
    else:
        if df_idx in excluded_cluster_map:
            cluster_index.append(excluded_cluster_map[df_idx])
        else:
            excluded_cluster_map[df_idx] = max_cluster_id
            cluster_index.append(max_cluster_id)
            max_cluster_id += 1

cluster_name = 'cluster_id'
input_df[cluster_name] = cluster_index

# Write out the data frame
input_df.to_csv(output_file)
print 'Dedupe complete, ran in ', time.time() - time_start, 'seconds'
