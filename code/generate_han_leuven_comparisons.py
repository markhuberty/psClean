#!/usr/bin/env
# -*- coding: utf-8 -*-
"""
Processes fung_id:other_id maps and generates aggregation statistics.
Takes command-line arguments for the maps themselves.

It assumes the command-line arguments come in the following order:
(1) the fung-leuven map
(2) the fung-han map

Both should be taken from the output of the crossdb_match.py file.

"""

import matplotlib.backends.backend_pdf as plt_pdf
import matplotlib.pyplot as plt
import numpy as np
import os
import pandas as pd
import sys

fung_han = sys.argv[-1]
fung_leuven = sys.argv[-2]

df_han = pd.read_csv(fung_han)
df_leuven = pd.read_csv(fung_leuven)

def count_summary_stats(id_map):
    """
    Given a Series or other array, returns the
    max/min/mean/median values. 
    """
    map_max = np.max(id_map)
    map_min = np.min(id_map)
    map_mean = np.mean(id_map)
    map_median = np.median(id_map)
    

    out =  {"max": map_max,
            "min": map_min,
            "mean": map_mean,
            "median": map_median
            }
    return out

def count_id_maps(df, id1, id2):
    """
    given a data frame, counts how many instances of id2 exist for id1.
    """
    this_df = df.set_index([id1, id2])

    grouped1 = this_df.groupby(level=0)
    grouped2 = this_df.groupby(level=1)

    id1_id2_map = grouped1.size()
    id2_id1_map = grouped2.size()

    id1_id2_stats = count_summary_stats(id1_id2_map)
    id2_id1_stats = count_summary_stats(id2_id1_map)

    id1_id2 = {'map': id1_id2_map, 'stats': id1_id2_stats}
    id2_id1 = {'map': id2_id1_map, 'stats': id2_id1_stats}
    return id1_id2, id2_id1


## Get the overall statistics
df_han_idx = df_han[['fung_id', 'han_id']].drop_duplicates()
df_leuven_idx = df_leuven[['fung_id', 'leuven_id']].drop_duplicates()
df_leuven_idx_l2 = df_leuven.ix[df_leuven.leuven_ld_level == 2][['fung_id', 'leuven_id']].drop_duplicates()

fung_han_map, han_fung_map = count_id_maps(df_han_idx, 'fung_id', 'han_id')
fung_leuven_map, leuven_fung_map = count_id_maps(df_leuven_idx_l2, 'fung_id', 'leuven_id')
fung_leuven_l2_map, leuven_l2_fung_map = count_id_maps(df_leuven_idx_l2, 'fung_id', 'leuven_id')

print 'Fung:HAN stats'
print fung_han_map['stats']
print 'Fung:Leuven stats'
print fung_leuven_map['stats']
print 'Fung:Leuven L2 stats'
print fung_leuven_l2_map['stats']

print 'HAN:Fung stats'
print han_fung_map['stats']
print 'Leuven:Fung stats'
print leuven_fung_map['stats']
print 'Leuven L2:Fung stats'
print leuven_l2_fung_map['stats']

def get_name_by_stat(input_df, idx, map_series, stats, stat_key='max', cols=None):
    """
    Given the id:id map counts and a statistic, return data from the original id
    map corresponding to that statistic.
    i.e., to get the names that correspond to the max id1:id2 count,
    stat_key should be 'max' and cols should include the name field
    """
    this_id = map_series[map_series == stats[stat_key]].index.values[0]
    df_out = input_df.ix[input_df[idx] == this_id]

    if cols:
        df_out = df_out[cols]
    return df_out.drop_duplicates()

df_max_fl = get_name_by_stat(df_leuven,
                             'fung_id',
                             fung_leuven_map['map'],
                             fung_leuven_map['stats'],
                             'max',
                             ['fung_id', 'leuven_id', 'name']
                             )
df_max_fl.to_csv('fung_leuven_max.csv', index=False)

df_max_lf = get_name_by_stat(df_leuven,
                             'leuven_id',
                             leuven_fung_map['map'],
                             leuven_fung_map['stats'],
                             'max',
                             ['fung_id', 'leuven_id', 'name']
                             )
df_max_lf.to_csv('leuven_fung_max.csv', index=False)

df_max_fl_l2 = get_name_by_stat(df_leuven.ix[df_leuven.leuven_ld_level == 2],
                                'fung_id',
                                fung_leuven_l2_map['map'],
                                fung_leuven_l2_map['stats'],
                                'max',
                                ['fung_id', 'leuven_id', 'name']
                                )
df_max_fl_l2.to_csv('fung_leuven_l2_max.csv', index=False)


df_max_lf_l2 = get_name_by_stat(df_leuven.ix[df_leuven.leuven_ld_level == 2],
                                'leuven_id',
                                leuven_l2_fung_map['map'],
                                leuven_l2_fung_map['stats'],
                                'max',
                                ['fung_id', 'leuven_id', 'name']
                                )
df_max_lf_l2.to_csv('leuven_l2_fung_max.csv', index=False)


df_max_fh = get_name_by_stat(df_han,
                             'fung_id',
                             fung_han_map['map'],
                             fung_han_map['stats'],
                             'max',
                             ['fung_id', 'han_id', 'han_name']
                             )
df_max_fh.to_csv('fung_han_max.csv', index=False)

df_max_hf = get_name_by_stat(df_han,
                             'han_id',
                             han_fung_map['map'],
                             han_fung_map['stats'],
                             'max',
                             ['fung_id', 'han_id', 'han_name']
                             )
df_max_hf.to_csv('han_fung_max.csv', index=False)

