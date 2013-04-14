#!/usr/bin/env
# -*- coding: utf-8 -*-

"""
Command-line script for matching the fung output to the HAN
and Leuven data and writing out the map.

Assumes the following argument order:
(1) the formatted fung input file (csv)
(2) the formatted fung output file (csv)
(3) the han file 
(4) the leuven file

Assumes the following fields in each file:
(fung input) unique_id, orig_id, name
(fung output) unique_id, orig_unique_id
(han) han_id, person_id, han_name
(leuven) name, person_id, hrm_l2_id, hrm_level, person_address, person_name
"""

import os
import pandas as pd
import string
import sys

sys.stdout = os.fdopen(sys.stdout.fileno(), 'wb', 0)

## Get the filenames off the command line
## NOTE: assumes that the fung outputs have
## already been processed with postprocess_fung_output.py

filenames = [f for idx, f in enumerate(sys.argv) if idx > 0]
fung_input_file = filenames[0]
fung_output_file = filenames[1]
han_data_file = filenames[2]
leuven_data_file = filenames[3]

print(fung_input_file,
      fung_output_file,
      han_data_file,
      leuven_data_file
      )

## Load the files
fung_output = pd.read_csv(fung_output_file)
fung_input = pd.read_csv(fung_input_file)
han_data = pd.read_csv(han_data_file)
leuven_data = pd.read_csv(leuven_data_file)

print fung_output.shape
print fung_input.shape
print han_data.shape
print leuven_data.shape

## Merge the fung inputs and outputs
fung_map = pd.merge(fung_input,
                    fung_output,
                    left_on='unique_id',
                    right_on='unique_id',
                    copy=False
                    )


print 'fung_map diagnostics:'
print fung_map.shape
print fung_map.columns


leuven_data =  leuven_data[['person_id',
                            'hrm_l2_id',
                            'hrm_level',
                            'person_address',
                            'person_name'
                            ]
                           ]
leuven_data.columns = ['person_id',
                       'leuven_id',
                       'leuven_ld_level',
                       'leuven_address',
                       'person_name'
                       ]



han_data = han_data[['HAN_ID','OCT11_Person_id','Person_name_clean']]
han_data.columns = ['han_id',
                    'person_id',
                    'han_name'
                    ]


leuven_map = pd.merge( leuven_data, fung_map, left_on = 'person_id', right_on = 'person_id', how='outer')
han_map = pd.merge( han_data, fung_map, left_on = 'person_id', right_on = 'person_id', how='outer')

leuven_map.to_csv('fung_leuven_map.csv', index=False)
han_map.to_csv('fung_han_map.csv', index=False)
