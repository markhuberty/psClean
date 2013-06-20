import pandas as pd
import numpy as np
import os

def shannon_entropy(vec):
    N = np.sum(vec)
    entropy = np.log(N) - (1.0/N) * np.sum(vec * np.log(vec))
    return entropy

base_dir = '/home/markhuberty/projects/psClean/data/weighted/'
files = os.listdir(base_dir)

def set_entropy(vec):
    len_vec = [len(s.split('**')) if isinstance(s, str) else 0 for s in vec]
    entropy = shannon_entropy(len_vec)
    return entropy


## Goal: get the entropy by ID, sort entropies by total patent_ct
## Look at how the entropy compares across countries. Missing BE, but oh well.
country_entropies = []
for f in files:
    print f
    df = pd.read_csv(base_dir + f)
    g = df.groupby(['cluster_id_r1', 'Name'])
    name_entropy = g.size().groupby(level=0).agg(shannon_entropy)

    g = df.groupby('cluster_id_r1')
    class_entropy = g.agg({'Class': set_entropy,
                          'Coauthor': set_entropy
                           }
                          )
    class_entropy['Name'] = name_entropy
    class_entropy['Country'] = f[:2]
    country_entropies.append(class_entropy)

df_entropy = pd.concat(country_entropies, axis=0, ignore_index=True)

g = df_entropy.groupby('Country')
test = g.agg(np.mean)
