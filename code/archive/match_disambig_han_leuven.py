import csv
import os
import pandas as pd
import string

os.chdir('/Users/markhuberty/Documents/Research/Papers/psClean/data')

## Want unique_id:actual_id df

def disambig_to_list(disambig_row,
                     delim_primary='###',
                     delim_secondary=','
                     ):
    """
    Takes one row in the fung disambiguator final output, of form
    unique_id###match_prob###match,id,values and returns a
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
    with open(disambig_out, 'rt') as infile:
        for row in infile:
            output_element = disambig_to_list(row,
                                              delim_primary,
                                              delim_secondary
                                              )
            df_input.extend(output_element)
    print df_input[0]
    df = pd.DataFrame(df_input, columns=['unique_id', 'orig_id'])
    return df


## Still super slow. Need something faster. Likely
## best to have a good dict of origid:uniqueid,
## then just do the O(1) lookup on that? bleh.
def compare_id_mapping(map_1, map_2):
    """
    map_* are pandas data frames of for unique_id:orig_id
    Returns the unique ids in 2 that map to the unique ids in 1;
    And a list of unique_ids in map_1 that map to > 1 unique_id in map_2
    """
    test = pd.merge(map_1, map_2, how='inner',
                left_on='person_id', right_on='person_id'
                )
    test_grouped = test.groupby('unique_id_x')

    group_counts = []
    multi_ids = []
    for name, group in test_grouped:
        ct = len(group.unique_id_y.drop_duplicates())
        group_counts.append([name, ct])
        if ct > 1:
            multi_ids.append(name)
    df_out = pd.DataFrame(group_counts, columns=['unique_id', 'ct'])
    return df_out, multi_ids


def return_multimapped_names(multi_obj, unique_id_obj, name_obj, threshold):
    bool_idx = [True if uid in multi_obj else False
                for uid in unique_id_obj
                ]

    """
    Given returns names corresponding to the ids provided in
    multi_obj, if they are found in unique_id_obj
    """
    df = pd.DataFrame({'uid': unique_id_obj[bool_idx],
                       'name': name_obj[bool_idx]
                       }
                      )
    grouped = df.groupby('uid')

    print 'computing name_sets'
    multi_names = {}
    for group_id, group in grouped:
        name_set = set(group.name)
        if len(name_set) > threshold:
            multi_names[group_id] = name_set

    return multi_names


## then just count it up
fung_input = 'nl_test_data.csv'
fung_filename = 'final.txt'
han_filename = 'han_nl_map.csv'
leuven_filename = 'netherlands_leuven.csv'

## For the fung input data, first subset it to
## only what we need here
with(open('nl_test_data_sub.csv', 'wt')) as f1:
    with open(fung_input, 'rt') as f2:
        writer = csv.DictWriter(f1, fieldnames=['unique_id', 'orig_id', 'name'])
        dreader = csv.DictReader(f2)
        for row in dreader:
            row_sub = {'unique_id': row['Unique_Record_ID'],
                       'orig_id': row['Person'],
                       'name': row['Name']
                       }
            writer.writerow(row_sub)

## Then load it
fung_orig = pd.read_csv('nl_test_data_sub.csv',
                        header=None,
                        names=['unique_id', 'person_id', 'name']
                        )

## Reformat the fung output
fung_list = disambig_to_df(fung_filename)

## Load the HAN and Leuven unique_id:person_id maps
han_list = pd.read_csv(han_filename)
leuven_list = pd.read_csv(leuven_filename)

## Attach the person_id to the fung_list
fung_map = pd.merge(fung_list,
                    fung_orig,
                    how='left',
                    left_on='orig_id',
                    right_on='unique_id'
                    )
fung_map = fung_map[['unique_id_x', 'orig_id_y']]


fung_map.columns = ['unique_id', 'person_id']
han_list.columns = ['unique_id', 'person_id']
leuven_list.columns = ['person_id', 'unique_id']

## Compute the fung_id:other_id maps and return
## unique fung_ids w/ > 1 other_id
fung_han_accuracy, fung_han_multi = compare_id_mapping(fung_map, han_list)
fung_leuven_accuracy, fung_leuven_multi = compare_id_mapping(fung_map, leuven_list)

pd.DataFrame(fung_han_accuracy,
             columns=['unique_id','ct_han_ids']).to_csv('fung_han_map.csv')
pd.DataFrame(fung_leuven_accuracy,
             columns=['unique_id', 'ct_leuven_ids']).to_csv('fung_leuven_map.csv')


## Merge the fung list into the original ids
## 
fung_all = pd.merge(fung_list,
                    fung_orig,
                    how='left',
                    left_on='orig_id',
                    right_on='unique_id'
                    )

# Generate a subset that handles only instances
# where the fung_id mapped to > 1 HAN id

multi_han = return_multimapped_names(fung_han_multi,
                                     fung_all.unique_id_x,
                                     fung_all.name,
                                     threshold=1
                                     )

multi_leuven = return_multimapped_names(fung_leuven_multi,
                                        fung_all.unique_id_x,
                                        fung_all.name,
                                        threshold=1
                                        )

## Write out id:name csv files of the names that were
## mapped to > 1 HAN or Leuven ID

with open('fung_multi_han_names.csv', 'wt') as f:
    writer = csv.writer(f)
    writer.writerow(['fung_id', 'name'])
    for k in multi_han:
        output_list = [[k, name] for name in multi_han[k]]
        for entity in output_list:
            writer.writerow(entity)

with open('fung_multi_leuven_names.csv', 'wt') as f:
    writer = csv.writer(f)
    writer.writerow(['fung_id', 'name'])
    for k in multi_leuven:
        output_list = [[k, name] for name in multi_leuven[k]]
        for entity in output_list:
            writer.writerow(entity)
