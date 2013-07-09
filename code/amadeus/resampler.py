import numpy as np

def random_sampler(d1, d2, N):
    """
    Given 2 sets of indices, return N random
    pairs w/0 repl
    """
    np.random.shuffle(d1)
    np.random.shuffle(d2)

    idx1 = np.random.choice(d1, N, replace=True)
    idx2 = np.random.choice(d2, N, replace=True)

    idx_pairs = zip(idx1, idx2)
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
