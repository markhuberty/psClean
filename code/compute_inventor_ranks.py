import pandas as pd
import numpy as np
from scipy.stats import spearmanr, pearsonr
import sys


def consolidate_unique(ser):
    """
    Consolidates a Pandas series by taking the most frequent variant in that
    series. Useful for things like ID:name maps. If there's only one value
    in the series, returns that value.
    """
    if len(ser) > 0:
        g_ser = ser.value_counts().order()
        return g_ser.index[-1]
    return ser[0]

# def consolidate_df(df, col='patent_ct'):
#     idx = df.index[np.argmax(df[col])]
#     return df.ix[idx]


# def consolidate_record_counts(df_id_patent, cluster_id='cluster_id_r1'):
#     ## Standardize on name:
#     grouped_name = stuff


#-------------------------------
# NAME STANDARDIZATION
#-------------------------------

def compute_canonical_names(df_ref, ref_cluster, d_cluster):
    """
    Given a dataframe of form ref_id:dedupe_id:person_name,
    returns the most frequent name variant for each unique leuven:dedupe id pair
    """
    g = df_ref[[ref_cluster, d_cluster, 'person_name']].groupby([ref_cluster, d_cluster])
    g_names = g.agg(consolidate_unique)
    g_names.reset_index(inplace=True)
    return g_names

def compute_canonical_counts(df_ref, df_p, ref_cluster='leuven_id', d_cluster='cluster_id_r1'):
    """
    Given a reference object with leuven_id:cluster_id mappings and a matching patent dataframe,
    returns max patent counts and canonical names for each unique ID pair.
    """
    canonical_names = compute_canonical_names(df_ref, ref_cluster, d_cluster)

    # Merge in the patent data and count patents by Leuven ID
    name_patent = pd.merge(canonical_names,
                           df_p[[d_cluster, ref_cluster, 'Name']],
                           left_on=[ref_cluster, d_cluster],
                           right_on=[ref_cluster, d_cluster],
                           how='inner'
                           )
    g_name_patent = name_patent.groupby([ref_cluster, d_cluster])
    name_patent_size = g_name_patent.size().reset_index()
    name_patent_size.columns = [ref_cluster, d_cluster, 'patent_ct']
    
    g_name_patent_size = name_patent_size.groupby(ref_cluster)
    canonical_ids = g_name_patent_size.apply(lambda t: t[t.patent_ct==t.patent_ct.max()].irow(0))
    canonical_ids.reset_index(inplace=True, drop=True)
    
    # Then reverse: consolidate the output by dedupe ID, again picking the largest
    # number of patents
    g_canonical_ids = canonical_ids.groupby(d_cluster)
    final_id_counts = g_canonical_ids.apply(lambda t: t[t.patent_ct==t.patent_ct.max()].irow(0))
    final_id_counts.reset_index(inplace=True, drop=True)

    # Then add on the actual patent counts by dedupe and leuven id
    dedupe_size = df_leuven_patent.groupby(d_cluster).size()
    ref_size = df_leuven_patent.groupby(ref_cluster).size()
    
    final_id_counts.set_index(d_cluster, inplace=True)
    final_id_counts['dedupe_ct'] = dedupe_size
    
    final_id_counts.reset_index(inplace=True)
    final_id_counts.set_index(ref_cluster, inplace=True)
    final_id_counts['ref_ct'] = ref_size
    
    final_id_counts.sort(columns=['dedupe_ct', 'ref_ct'], ascending=False, inplace=True)
    final_id_counts['ratio'] = final_id_counts['dedupe_ct'] / final_id_counts['ref_ct'].astype('float')
    final_id_counts.reset_index(inplace=True)
    
    out = pd.merge(final_id_counts,
                   canonical_names,
                   left_on=[ref_cluster, d_cluster],
                   right_on=[ref_cluster, d_cluster],
                   how='left'
                   )

    return out[[ref_cluster, d_cluster, 'dedupe_ct', 'ref_ct']]


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
inputs = [i for idx, i in enumerate(sys.argv) if idx > 0]
patent_dir = inputs[0]
person_dir = inputs[1]
output_dir = inputs[2]

pearsons = []
spearmans = []
cids = []
countries = []

for country in eu27:

    try:
        df_leuven = pd.read_csv(output_dir + country + '/' + 'dedupe_leuven_map.csv')
        df_patent = pd.read_csv(patent_dir + country + '_person_patent_map.csv')
        df_input = pd.read_csv(person_dir + 'dedupe_input_' + country + '.csv')
    except:
        continue

    df_input = df_input[['Person', 'Name']]
    df_leuven = df_leuven.dropna()

    df_leuven_patent = pd.merge(df_leuven,
                                df_patent,
                                left_on='Person',
                                right_on='Person',
                                how='inner'
                                )

    df_leuven_patent = pd.merge(df_leuven_patent,
                                df_input,
                                left_on='Person',
                                right_on='Person',
                                how='inner'
                                )

    df_leuven_patent = df_leuven_patent[['cluster_id_r1',
                                         'cluster_id_r2',
                                         'leuven_id',
                                         'leuven_ld_level',
                                         'Name'
                                         ]]

    leuven_d1 = compute_canonical_counts(df_leuven,
                                         df_leuven_patent,
                                         ref_cluster='leuven_id',
                                         d_cluster='cluster_id_r1'
                                         )

    leuven_d2 = compute_canonical_counts(df_leuven,
                                         df_leuven_patent,
                                         ref_cluster='leuven_id',
                                         d_cluster='cluster_id_r2'
                                         )

    d1_spearman = spearmanr(leuven_d1.ref_ct, leuven_d1.dedupe_ct)
    d1_pearson = pearsonr(leuven_d1.ref_ct, leuven_d1.dedupe_ct)

    d2_spearman = spearmanr(leuven_d2.ref_ct, leuven_d2.dedupe_ct)
    d2_pearson = pearsonr(leuven_d2.ref_ct, leuven_d2.dedupe_ct)

    pearsons.append(d1_pearson)
    spearmans.append(d1_spearman)
    cids.append('round1')
    countries.append(country)

    pearsons.append(d2_pearson)
    spearmans.append(d2_spearman)
    cids.append('round2')
    countries.append(country)

    d1_output_file = output_dir + country + '/dedupe_leuven_patent_counts_r1.csv'
    leuven_d1.to_csv(d1_output_file)

    d2_output_file = output_dir + country + '/dedupe_leuven_patent_counts_r2.csv'
    leuven_d2.to_csv(d2_output_file)

    print 'finished with %s' % country
    
df_corr = pd.DataFrame({'country': countries,
                        'cluster': cids,
                        'pearson': pearsons,
                        'spearman': spearmans
                        }
                       )
df_corr.to_csv('eu27_dedupe_leuven_patent_ct_corr.csv')
