import pandas as pd
import operator
import random

def consolidate_unique(x):
    return x.values[0]

def consolidate_geo(x):
    geo_counts = {}
    for g in x.values:
        try:
            g = float(g)
        except TypeError:
            g = 0.0
        if g != 0.0:
            if g in geo_counts:
                geo_counts[g] += 1
            else:
                geo_counts[g] = 1
    if len(geo_counts) > 0:
        sorted_geo = sorted(geo_counts.iteritems(),
                            key=operator.itemgetter(1),
                            reverse=True
                            )
        return sorted_geo[0][0]
    else:
        return 0.0

def consolidate_set(x, delim='**', maxlen=100):
    """
    Consolidates all multi-valued strings in x
    into a unique set of maximum length maxlen.

    Returns a multivalued string separated by delim
    """
    vals = [v.split(delim) for v in x.values if isinstance(v, str)]
    val_set = [v for vset in vals for v in vset]
    val_set = list(set(val_set))
    if len(val_set) > 0:
        if len(val_set) > maxlen:
            rand_idx = random.sample(range(len(val_set)), maxlen)
            val_set = [val_set[idx] for idx in rand_idx]
        out = delim.join(val_set)
    else:
        out = ''
    return out

def consolidate(df, key, agg_dict):
    grouped = df.groupby(key)
    records = grouped.agg(agg_dict)
    return records
