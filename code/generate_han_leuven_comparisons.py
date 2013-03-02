#!/usr/bin/env
# -*- coding: utf-8 -*-
"""
Processes fung_id:other_id maps and generates aggregation statistics.
Takes command-line arguments for the maps themselves.

It assumes the command-line arguments come in the following order:
(1) the fung-han map
(2) the fung-leuven map

Both should be taken from the output of the crossdb_match.py file.

"""

import pandas as pd
import os
import sys
import numpy as np

fung_han = sys.argv[-1]
fung_leuven = sys.argv[-2]

df_han = pd.read_csv(fung_han)
df_leuven = pd.read_csv(fung_leuven)

def count_summary_stats(id_map):
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


## TODO: this needs work on exactly what the output will
## look like.
## Want the following: for unique second-tier leuven IDs,
## how many PATSTAT person_ids got a unique fung_id?
## And v/v where did we split? Where did we over-aggregate?
## to do this: aggregate by <id> + person_id, count size.
## Then merge on person_id, so now we've got counts for leuven and han.
## Then compare. 
def estimate_precision_recall(input_df, new_id='fung_id', benchmark_id='leuven_id'):
    """
    Estimates the precision/recall data compared to the Leuven hand-checked data
    """
    ## Want: number of fung ids that map to a single leuven id
    ## How many unique fung_ids map to one unique leuven_id?
    ## How many unique fung_ids map to > 1 leuven_id? (clumping)
    ## How many unique fung_ids map to < 1 leuven_id (splitting)
    ## How many of the person_ids assigned to a single leuven_id do we assign to a single fung_id
    ## How many of the person_ids assigned to a single fung_id to we assign to a single fung_id?

    input_df_sub = input_df[[new_id, benchmark_id]].drop_duplicates()
    df_sub.reset_index(inplace=True)
    input_df.reset_index(inplace=True)
    input_df.set_index([new_id, benchmark_id])
    df_sub.set_index([new_id, benchmark_id])

    ## Look at just the IDs:
    grouped = df_sub.groupby(level=0)
    new_id_benchmark_id_size = grouped.size()

    grouped = df_sub.groupby(level=1)
    benchmark_id_new_id_size = grouped.size()

    ct_new_id_benchmark_id_unique = np.sum(new_id_benchmark_id_size == 1)
    ct_new_id_benchmark_id_split = np.sum(new_id_benchmark_id_size > 1)

    ct_benchmark_id_new_id_unique = np.sum(benchmark_id_new_id_size == 1)
    ct_benchmark_id_new_id_split = np.sum(new_id_benchmark_id_size > 1)

    ## Now how badly do we split or clump?

    split_index = new_id_benchmark_id_size.index[new_id_bechmark_id_size > 1]
    clump_index = benchmark_id_new_id_size.index[benchmark_id_new_id_size > 1]

    input_df.index([new_id, benchmark_id])

    df_split = input_df.ix[split_index]
    df_clump = input_df.ix[clump_index]
                             
    

## Get the overall statistics
df_han_idx = df_han[['fung_id', 'han_id']].drop_duplicates()
df_leuven_idx = df_leuven[['fung_id', 'leuven_id']].drop_duplicates()
df_leuven_idx_l2 = df_leuven.ix[df_leuven.leuven_ld_level == 2][['fung_id', 'leuven_id']].drop_duplicates()

fung_han_map, han_fung_map = count_id_maps(df_han_idx, 'fung_id', 'han_id')
fung_leuven_map, leuven_fung_map = count_id_maps(df_leuven_idx_l2, 'fung_id', 'leuven_id')
fung_leuven_l2_map, leuven_l2_fung_map = count_id_maps(df_leuven_idx_l2, 'fung_id', 'leuven_id')


def get_name_by_stat(input_df, idx, map_series, stats, stat_key='max', cols=None):
    this_id = map_series[map_series == stats[stat_key]].index.values[0]
    df_out = input_df.ix[input_df[idx] == this_id]

    if cols:
        df_out = df_out[cols]
    return df_out

df_max_fl = get_name_by_stat(df_leuven,
                             'fung_id',
                             fung_leuven_map['map'],
                             fung_leuven_map['stats'],
                             'max',
                             ['fung_id', 'leuven_id', 'person_name']
                             )

df_max_lf = get_name_by_stat(df_leuven,
                             'leuven_id',
                             leuven_fung_map['map'],
                             leuven_fung_map['stats'],
                             'max',
                             ['fung_id', 'leuven_id', 'person_name']
                             )

df_max_fl_l2 = get_name_by_stat(df_leuven.ix[df_leuven.leuven_ld_level == 2],
                                'fung_id',
                                fung_leuven_l2_map['map'],
                                fung_leuven_l2_map['stats'],
                                'max',
                                ['fung_id', 'leuven_id', 'person_name']
                                )

df_max_lf_l2 = get_name_by_stat(df_leuven.ix[df_leuven.leuven_ld_level == 2],
                                'leuven_id',
                                leuven_l2_fung_map['map'],
                                leuven_l2_fung_map['stats'],
                                'max',
                                ['fung_id', 'leuven_id', 'person_name']
                                )

df_max_fh = get_name_by_stat(df_han,
                             'fung_id',
                             fung_han_map['map'],
                             fung_han_map['stats'],
                             'max',
                             ['fung_id', 'leuven_id', 'person_name']
                             )

df_max_hf = get_name_by_stat(df_han,
                             'han_id',
                             han_fung_map['map'],
                             han_fung_map['stats'],
                             'max',
                             ['fung_id', 'leuven_id', 'person_name']
                             )


