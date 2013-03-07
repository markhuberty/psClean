#!/usr/bin/env
# -*- coding: utf-8 -*-
"""
Command-line script for parsing the fung input and output files
for comparison elsewhere.

Assumes command-line arguments in the following order:
(1) the fung disambiguator input file
(2) the fung disambiguator output file, in native format

The fung disambiguator input file must have the following
fieldnames: Unique_Record_ID, Person, Name
"""
import csv
import os
import pandas as pd
import re
import string
import sys

sys.stdout = os.fdopen(sys.stdout.fileno(), 'wb', 0)

def disambig_to_list(disambig_row,
                     delim_primary='###',
                     delim_secondary=','
                     ):
    """
    Takes one row in the fung disambiguator final output, of form
    unique_id###match_prob###match_id,values and returns a
    list of form [(unique_id, match_id), ]
    """
    primary_split = disambig_row.split(delim_primary)
    matching_ids = primary_split[2].rstrip(',\n').split(delim_secondary)
    input_list = [(int(primary_split[0]), int(mid)) for mid in matching_ids]

    return input_list

def disambig_to_df(disambig_out,
                   delim_primary='###',
                   delim_secondary=','
                   ):
    """
    Reformats the fung disambuguator output to a python
    data frame of form unique_id:match_id
    
    """
    df_input = []
    with open(disambig_out, 'r') as infile:
        for row in infile:
            output_element = disambig_to_list(row,
                                              delim_primary,
                                              delim_secondary
                                              )
            df_input.extend(output_element)
    print df_input[0]
    df = pd.DataFrame(df_input, columns=['fung_id', 'unique_id'])
    return df

def re_filename(filename):
    out = re.sub('[a-z]+$', 'reformat', filename)
    return out

read_disambig_input = sys.argv[-2]
read_disambig_output = sys.argv[-1]

write_disambig_input = re_filename(read_disambig_input)
write_disambig_output = re_filename(read_disambig_output)

print(read_disambig_input,
      read_disambig_output,
      write_disambig_input,
      write_disambig_output
      )

fung_cleaned = disambig_to_df(read_disambig_output)
fung_cleaned.to_csv(write_disambig_output, index=False)

del fung_cleaned
print 'Output cleaned'

## For the fung input data, first subset it to
## only what we need here
with(open(write_disambig_input, 'wt')) as f1:
    with open(read_disambig_input, 'rt') as f2:
        writer = csv.DictWriter(f1, fieldnames=['unique_id', 'person_id', 'name'])
        headers = dict(zip(writer.fieldnames, writer.fieldnames))
        writer.writerow(headers)
        dreader = csv.DictReader(f2)
        for row in dreader:
            row_sub = {'unique_id': row['Unique_Record_ID'],
                       'person_id': row['Person'],
                       'name': row['Name']
                       }
            writer.writerow(row_sub)



