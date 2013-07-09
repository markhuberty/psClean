import numpy as np
import math
import dedupe

def random_sampler(d1, d2, N):
    """
    Given 2 sets of indices, return N random
    pairs w/0 repl
    """

    n_across = int(math.ceil(0.75 * N))
    n_within = int(math.ceil(0.125 * N))
    np.random.shuffle(d1)
    np.random.shuffle(d2)

    if n_across <= len(d1) and n_across <= len(d2):
        idx1 = d1[:n_across]
        idx2 = d2[:n_across]
    else:
        idx1 = np.random.choice(d1, n_across, replace=True)
        idx2 = np.random.choice(d2, n_across, replace=True)

    # Then walk in some within-db stuff so we learn to reject it
    within_samples1 = within_sampler(d1, n_within)
    within_samples2 = within_sampler(d2, n_within)
    
    idx_pairs = zip(idx1, idx2)
    idx_pairs.extend(within_samples1)
    idx_pairs.extend(within_samples2)
    np.random.shuffle(idx_pairs)
    return idx_pairs


def split_dataSample(data_d, N):
    """
    Given a dedupe data object with 2 sources, split the sources and return
    random pairs where each pair has a record from both sources
    """
    d1 = [d for d in data_d if data_d[d]['dbase']=='amadeus']
    d2 = [d for d in data_d if data_d[d]['dbase']=='patstat']

    idx = random_sampler(d1, d2, N)

    data_sample = [(data_d[idx1], data_d[idx2]) for idx1, idx2 in idx]
    return data_sample


def within_sampler(d, N):

    min_d = np.min(d)
    idx_pairs = dedupe.core.randomPairs(len(d), N)
    out = [(a + min_d, b + min_d) for a, b in idx_pairs]
    return out
