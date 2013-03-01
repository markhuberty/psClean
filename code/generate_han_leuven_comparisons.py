#!/usr/bin/env
# -*- coding: utf-8 -*-


import pandas as pd
import os
import sys
import numpy as np

input_file = sys.argv[-1]

df = pd.read_csv(input_file)

def count_summary_stats(id_map):
    map_max = np.max(id_map)
    map_min = np.max(id_map)
    map_mean = np.mean(id_map)
    map_median = np.median(id_map)

    out =  {"max": map_max,
            "min": map_min,
            "mean": map_mean,
            "median": map_median
            }
    return out

def count_id_maps(df, id1, id2):
    df.index([id1, id2])

    grouped1 = df.groupby(level=0)
    grouped2 = df.groupby(level=1)

    id1_id2_map = grouped1.size()
    id2_id1_map = grouped2.size()

    id1_id2_stats = count_summary_stats(id1_id2_map)
    id2_id1_stats = count_summary_stats(id2_id1_map)

    return id1_id2_stats, id2_id1_stats


## TODO: this needs work on exactly what the output will
## look like.
## Want the following: for unique second-tier leuven IDs,
## how many PATSTAT person_ids got a unique fung_id?
## And v/v where did we split? Where did we over-aggregate?
## to do this: aggregate by <id> + person_id, count size.
## Then merge on person_id, so now we've got counts for leuven and han.
## Then compare. 
def estimate_precision_recall(df, new_id='fung_id', benchmark_id='leuven_id'):
    """
    Estimates the precision/recall data compared to the Leuven hand-checked data
    """
    ## Want: number of fung ids that map to a single leuven id
    ## How many unique fung_ids map to one unique leuven_id?
    ## How many unique fung_ids map to > 1 leuven_id? (clumping)
    ## How many unique fung_ids map to < 1 leuven_id (splitting)
    ## How many of the person_ids assigned to a single leuven_id do we assign to a single fung_id
    ## How many of the person_ids assigned to a single fung_id to we assign to a single fung_id?

    df_sub = df[[new_id, benchmark_id]].drop_duplicates()
    df_sub.reset_index(inplace=True)
    df.reset_index(inplace=True)
    df.set_index([new_id, benchmark_id])
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

    df.index([new_id, benchmark_id])

    df_split = df.ix[split_index]
    df_clump = df.ix[clump_index]
                             
    

## Get the overall statistics
fung_han_stats, han_fung_stats = count_id_maps(df, 'fung_id', 'han_id')
fung_leuven_stats, leuven_fung_stats = count_id_maps(df, 'fung_id', 'leuven_id')

## Then subset to only the leuven level 2 stats
df_sub = df[df.leuven_id_level == 2]
fung_leuven_l2_stats, leuven_fung_l2_stats = count_id_maps(df_sub, 'fung_id', 'leuven_id')
