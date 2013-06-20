# In[36]:
import numpy as np
import os
import pandas as pd
import re
import sys

def name_matches(id_map, map_name, name_string, max_rows):
    name_regex = re.compile(name_string)
    names = [val if isinstance(val, str) else '' for val in id_map[map_name].values]
    name_bool = [True if name_regex.search(val) else False for val in names]
    id_sub = id_map[name_bool]

    if max_rows:
        id_sub = id_sub[:max_rows]
    return id_sub.drop_duplicates()

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

def compute_value_pct(dedupe_map):
    agg = dedupe_map.value_counts()
    pct = agg / float(np.sum(agg))
    return np.round(pct, 3)

def compute_max_share(g):
    g_max = g.max()
    g_tot = g.sum()
    return g_max / g_tot
    #return g_share

def compute_overall_pct(g):
    g_tot = np.sum(g)
    g_share = g / float(g_tot)
    return g_share

def compute_patent_pct(g):
    g_max = g.max()
    g_tot = g.sum()
    return g_max / g_tot
    #return g_share

def compute_overall_pct(g):
    g_tot = np.sum(g)
    g_share = g / float(g_tot)
    return g_share


eu27 = ['at',
        'bg',
        'be',
        'it',
        'gb',
        'fr',
        'de',
        'sk',
        'se',
        'pt',
        'pl',
        'hu',
        'ie',
        'ee',
        'es',
        'cy',
        'cz',
        'nl',
        'si',
        'ro',
        'dk',
        'lt',
        'lu',
        'lv',
        'mt',
        'fi',
        'el',
        ]

# Pull in the targets
inputs = [i for idx, i in enumerate(sys.argv) if idx > 0]
root_dir = inputs[0]

cluster_ids = ['cluster_id_r1']#, 'cluster_id_r2']

id_precision = []
id_recall = []
l1_patent_precision = []
l1_patent_recall = []
l2_patent_precision = []
l2_patent_recall = []
pid_count = []
id1_count = []
id2_count = []
country_index = []
cluster_label = []

for cluster_id in cluster_ids:
    for country in eu27:
        input_dir = os.path.expanduser(root_dir + country + '_weighted/')

        # Define the input filenames
        dedupe_han = input_dir + 'dedupe_han_map.csv'
        dedupe_leuven = input_dir + 'dedupe_leuven_map.csv'
        person_patent = os.path.expanduser('~/Documents/psClean/data/dedupe_input/person_patent/' + country + '_person_patent_map.csv')
        # cluster_id = 'cluster_id_r2'

        # Load the input files
        try:
            df_han = pd.read_csv(dedupe_han)
            df_leuven = pd.read_csv(dedupe_leuven)
            df_pp = pd.read_csv(person_patent)
        except:
            continue

        country_index.append(country)
        cluster_label.append(cluster_id)

        # # Person ID recall and precision
        df_leuven_2 = df_leuven[df_leuven.leuven_ld_level == 2]
        dedupe_na = np.isnan(df_leuven_2.cluster_id_r2)
        df_leuven_2 = df_leuven_2.ix[~dedupe_na]

        if df_leuven_2.shape[0] > 0:
            df_leuven_2 = df_leuven_2[[cluster_id, 'leuven_id', 'person_id']]
            grouped_both = df_leuven_2.groupby(['leuven_id', cluster_id])
            leuven_dedupe_ct = grouped_both.size()

            grouped_leuven_ct = leuven_dedupe_ct.groupby(level=0)
            max_vals = grouped_leuven_ct.agg(np.max)
            overall_recall = np.sum(max_vals) / float(np.sum(leuven_dedupe_ct))
            id_recall.append(overall_recall)

            grouped_dedupe_ct = leuven_dedupe_ct.groupby(level=1)
            max_vals = grouped_dedupe_ct.agg(np.max)
            overall_precision = float(np.sum(max_vals)) / np.sum(leuven_dedupe_ct)
            id_precision.append(overall_precision)
        else:
            id_recall.append('NA')
            id_precision.append('NA')

        # Then compute precision/recall at the patent level
        df_han_pp = pd.merge(df_han, df_pp, how='inner', left_on='person_id', right_on='Person')
        df_leuven_pp = pd.merge(df_leuven, df_pp, how='inner', left_on='person_id', right_on='Person')
        df_leuven_pp_l2 = df_leuven_pp[df_leuven_pp.leuven_ld_level == 2]


        grouped_han_pp = df_han_pp[[cluster_id, 'han_id', 'Patent']].groupby([cluster_id, 'han_id'])
        grouped_leuven_pp = df_leuven_pp[[cluster_id, 'leuven_id', 'Patent']].groupby([cluster_id, 'leuven_id'])

        han_dedupe_patent_ct = grouped_han_pp.size()
        leuven_dedupe_patent_ct = grouped_leuven_pp.size()
        
        grouped_han_ct = han_dedupe_patent_ct.groupby(level=1)
        grouped_leuven_ct = leuven_dedupe_patent_ct.groupby(level=1)

        overall_recall = np.sum(grouped_leuven_ct.agg(np.max)) / float(np.sum(leuven_dedupe_patent_ct))
        grouped_dedupe_ct = leuven_dedupe_patent_ct.groupby(level=0)
        overall_precision = np.sum(grouped_dedupe_ct.agg(np.max)) / float(np.sum(leuven_dedupe_patent_ct))


        if df_leuven_pp_l2.shape[0] > 0:
            grouped_leuven_pp_l2 = df_leuven_pp_l2[[cluster_id, 'leuven_id', 'Patent']].groupby([cluster_id, 'leuven_id'])
            leuven_l2_dedupe_patent_ct = grouped_leuven_pp_l2.size()
            grouped_leuven_l2_ct = leuven_l2_dedupe_patent_ct.groupby(level=1)
            l2_overall_recall = np.sum(grouped_leuven_l2_ct.agg(np.max)) / float(np.sum(leuven_l2_dedupe_patent_ct))
            grouped_dedupe_ct = leuven_l2_dedupe_patent_ct.groupby(level=0)
            l2_overall_precision = np.sum(grouped_dedupe_ct.agg(np.max)) / float(np.sum(leuven_l2_dedupe_patent_ct))

        else:
            l2_overall_recall = 'NA'
            l2_overall_precision = 'NA'
            
        l1_patent_recall.append(overall_recall)
        l1_patent_precision.append(overall_precision)
        l2_patent_recall.append(l2_overall_recall)
        l2_patent_precision.append(l2_overall_precision)

        df_dedupe = df_leuven[['Person', 'cluster_id_r1']].drop_duplicates()
        n_pid = len(df_dedupe.Person.drop_duplicates())
        n_id1 = len(df_dedupe.cluster_id_r1.drop_duplicates())
        #n_id2 = len(df_dedupe.cluster_id_r2.drop_duplicates())
        pid_count.append(n_pid)
        id1_count.append(n_id1)
        #id2_count.append(n_id2)


    pr_dict = {'cluster_label': cluster_label,
               'id_precision': id_precision,
               'id_recall': id_recall,
               'l1_patent_recall': l1_patent_recall,
               'l2_patent_recall': l2_patent_recall,
               'l1_patent_precision': l1_patent_precision,
               'l2_patent_precision': l2_patent_precision
               }
    id_dict = {'cluster_label': cluster_label,
               'patstat': pid_count,
               'round1': id1_count#,
               #'round2': id2_count
               }

    pr_out = pd.DataFrame(pr_dict, index=country_index)
    id_out = pd.DataFrame(id_dict, index=country_index)
    pr_out.to_csv('patstat_country_precision_recall.csv', index=True)
    id_out.to_csv('patstat_dedupe_id_counts.csv', index=True)
