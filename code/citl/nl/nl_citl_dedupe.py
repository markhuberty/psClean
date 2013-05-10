#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Template for cross-database deduplication of PATSTAT data and
CITL emissions account data.

This code assumes that you have (1) a disambiguated PATSTAT file and
(2) a clean CITL file that have been concatenated; and a field called 'source' that
specifies which records come from which. It then disambiguates and preserves only
those matches that match _across_ databases.
"""

import os
import csv
import re
import collections
import logging
import optparse
#import patent_util # Custom
import sys
sys.path.append('/home/markhuberty/Documents/dedupe/examples/patent_example')
import AsciiDammit
from patent_util import preProcess
import pandas as pd

import numpy as np
import dedupe

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
        if isinstance(dfrow['name'], str):
            name = preProcess(dfrow['name'])
        else:
            name = ''
        row_out['latlong'] = (float(dfrow['lat']), float(dfrow['lng']))
        row_out['name'] = name
        #row_out['source'] = dfrow['source']
        row_tuple = [(k, v) for (k, v) in row_out.items()]
        data_d[idx] = dedupe.core.frozendict(row_tuple)
            
    return data_d

# ## Logging

# Dedupe uses Python logging to show or suppress verbose output. Added for convenience.
# To enable verbose logging, run `python examples/csv_example/csv_example.py -v`

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


# ## Setup

# Define input, output, and training file options
country = 'nl'
input_file = country + '_citl_input.csv'
output_file = country + '_citl_out.csv'
settings_file = country + '_citl_learned_settings'
training_file = country + 'citl_training.json'

import random
def generateTrainingSamples(d, n_pairs=1000):
    citl_records = dict((record, d[record]) for record in d if d[record]['source']=='citl')
    patstat_records = dict((record, d[record]) for record in d if d[record]['source']=='patstat')

    citl_idx = np.random.choice(citl_records.keys(), n_pairs, replace=True)
    patstat_idx = np.random.choice(patstat_records.keys(), n_pairs, replace=True)

    citl_sample = [(idx, citl_records[idx]) for idx in citl_idx] 
    patstat_sample = [(idx, patstat_records[idx]) for idx in patstat_idx]

    citl_shuffle = random.sample(citl_sample, n_pairs / 10)
    patstat_shuffle = random.sample(patstat_sample, n_pairs / 10)

    left_sample = citl_sample + citl_shuffle + patstat_shuffle
    right_sample = patstat_sample + citl_sample[:len(citl_shuffle)] + patstat_sample[:len(patstat_shuffle)]

    out = zip(right_sample, left_sample)
    return out

# Dedupe can take custom field comparison functions, here's one
# we'll use for zipcodes
def sameOrNotComparator(field_1, field_2) :
    if field_1 == field_2 :
        return 1
    else:
        return 0

print 'importing data ...'
input_df = pd.read_csv(input_file)
data_d = readDataFrame(input_df)


# ## Training

if os.path.exists(settings_file):
    print 'reading from', settings_file
    deduper = dedupe.Dedupe(settings_file)

else:
    # To train dedupe, we feed it a random sample of records.
    # We have to make sure that we include lots of CITL records here
    # to provide for good matches
    data_sample = dedupe.dataSample(data_d, 3 * len(data_d))


    fields = {
        'name': {'type': 'String'},
        'latlong': {'type': 'LatLong', 'Has Missing': True}
        }

    # Create a new deduper object and pass our data model to it.
    deduper = dedupe.Dedupe(fields)

    # If we have training data saved from a previous run of dedupe,
    # look for it an load it in.
    # __Note:__ if you want to train from scratch, delete the training_file
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

# ## Blocking

print 'blocking...'
# Initialize our blocker. We'll learn our blocking rules if we haven't
# loaded them from a saved settings file.
blocker = deduper.blockingFunction(ppc=0.001, uncovered_dupes=5)

# Save our weights and predicates to disk.  If the settings file
# exists, we will skip all the training and learning next time we run
# this file.
deduper.writeSettings(settings_file)

# Load all the original data in to memory and place
# them in to blocks. Each record can be blocked in many ways, so for
# larger data, memory will be a limiting factor.

blocked_data = dedupe.blockData(data_d, blocker)

# ## Clustering

# Find the threshold that will maximize a weighted average of our precision and recall. 
# When we set the recall weight to 2, we are saying we care twice as much
# about recall as we do precision.
#
# If we had more data, we would not pass in all the blocked data into
# this function but a representative sample.

threshold = deduper.goodThreshold(blocked_data, recall_weight=0.5)

# `duplicateClusters` will return sets of record IDs that dedupe
# believes are all referring to the same entity.

print 'clustering...'
clustered_dupes = deduper.duplicateClusters(blocked_data, threshold)

print '# duplicate sets', len(clustered_dupes)

# ## Writing Results

# Write our original data back out to a CSV with a new column called 
# 'Cluster ID' which indicates which records refer to each other.

cluster_membership = collections.defaultdict(lambda : 'x')
for (cluster_id, cluster) in enumerate(clustered_dupes):
    for record_id in cluster:
        cluster_membership[record_id] = cluster_id


with open(output_file, 'w') as f:
    writer = csv.writer(f)

    with open(input_file) as f_input :
        reader = csv.reader(f_input)

        heading_row = reader.next()
        heading_row.insert(0, 'Cluster ID')
        writer.writerow(heading_row)

        for row in reader:
            row_id = int(row[0])
            cluster_id = cluster_membership[row_id]
            row.insert(0, cluster_id)
            writer.writerow(row)
