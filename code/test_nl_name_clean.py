import pandas as pd
import time
import modifications

## Read in the original file
nl_orig = pd.read_csv('/mnt/db_master/patstat_raw/fleming_inputs/cleaned_output_NL.tsv')
nl_fung = pd.read_csv('/mnt/db_master/patstat_raw/disambig_input_data/nl_test_data/csv')

nl_fung = nl_fung[['Unique_Record_ID', 'Lat', 'Lng']]


## Read in the file used for the disambig itself

## Subset the disambig file to contain only the unique_id and the
## lat/long

## Merge them

## Check for > 1 name in the name field

nl_data = do_all(nl_data)


## Check for addresses in the address field


nl_data = do_addresses(nl_data, 'NL')


## Write out.


