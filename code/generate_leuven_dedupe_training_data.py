import pandas as pd
import json
import numpy as np

## Use the leuven hand-labeled data to generate
## training pairs
df_leuven = pd.read_csv('../data/leuven2011.csv')
df_leuven = df_leuven[df_leuven.columns[1:]]
df_leuven = df_leuven[df_leuven.hrm_level == 1]

df_patstat = pd.read_csv('../data/nl_test_data_5March2013.csv')

df_patstat.Name.fillna('', inplace=True)
df_patstat.Coauthor.fillna('', inplace=True)
df_patstat.Class.fillna('', inplace=True)

## Group the records by unique ID
grouped = df_leuven.groupby('hrm_l2_id')

## Generate the ID sizes
id_sizes = grouped.size()

## Generate pa
clumped = id_sizes[id_sizes > 1]
individual = id_sizes[id_sizes == 1]

def generate_matching_dicts(df_pair):
    dict_pairs = []
    for i, idx1 in enumerate(df_pair.index):
        for idx2 in df_pair.index[(i + 1):]:
            item1 = df_pair.ix[idx1].to_dict()
            #item1['LatLong'] = (item1['Lat'], item1['Lng'])
            item1['LatLong'] = str(item1['Lat']) + '**' + str(item1['Lng'])
            item1 = cast_serializable_ints(item1)
            item2 = df_pair.ix[idx2].to_dict()
            #item2['LatLong'] = (item1['Lat'], item2['Lng'])
            item2['LatLong'] = str(item2['Lat']) + '**' + str(item2['Lng'])
            del item1['Lat'], item1['Lng'], item2['Lat'], item2['Lng']
            item2 = cast_serializable_ints(item2)
            dict_pairs.append([item1, item2])
    return dict_pairs

def generate_nonmatching_dicts(df1, df2):
    dict_pairs = []
    for idx1 in df1.index:
        for idx2 in df2.index:
            item1 = df1.ix[idx1].to_dict()
            item1['LatLong'] = str(item1['Lat']) + '**' + str(item1['Lng'])
            # item1['LatLong'] = (item1['Lat'], item1['Lng'])
            item1 = cast_serializable_ints(item1)
            item2 = df2.ix[idx2].to_dict()
            item2['LatLong'] = str(item2['Lat']) + '**' + str(item2['Lng'])
            # item2['LatLong'] = (item1['Lat'], item2['Lng'])
            del item1['Lat'], item1['Lng'], item2['Lat'], item2['Lng']
            item2 = cast_serializable_ints(item2)
            dict_pairs.append([item1, item2])
    return dict_pairs

def cast_serializable_ints(d):
    for k in d:
        if isinstance(d[k], np.int64):
            d[k] = int(d[k])
    return d


## Serialize the clumped data into pairs
matching_pairs = []
max_pairs = 2000
for idx in clumped.index:
    person_ids = df_leuven.person_id[df_leuven.hrm_l2_id == idx]
    orig_data = df_patstat[df_patstat.Person.isin(person_ids)]
    if orig_data.shape[0] > 0:
        pair_list = generate_matching_dicts(orig_data)
        matching_pairs.extend(pair_list)
    if len(matching_pairs) > max_pairs:
        break

nonmatching_pairs = []
for i, idx in enumerate(clumped.index):
    ids_1 = df_leuven.person_id[df_leuven.hrm_l2_id == idx]
    df1 = df_patstat[df_patstat.Person.isin(ids_1)]
    if df1.shape[0] > 0:
        for jdx in clumped.index[(i + 1):]:
            ids_2 = df_leuven.person_id[df_leuven.hrm_l2_id == jdx]
            df2 = df_patstat[df_patstat.Person.isin(ids_2)]
            if df2.shape[0] > 0:
                pair_list = generate_nonmatching_dicts(df1, df2)
                nonmatching_pairs.extend(pair_list)
            else:
                continue
            if len(nonmatching_pairs) > max_pairs:
                break
    else:
        continue
    if len(nonmatching_pairs) > max_pairs:
        break


    
training_dict = {0: nonmatching_pairs, 1:matching_pairs}

with open('../data/patstat_example_training.json', 'wt') as f:
    json.dump(training_dict, f)
    
