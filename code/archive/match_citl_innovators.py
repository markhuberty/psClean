import pandas as pd
import csv
from dedupe.affinegap import normalizedAffineGapDistance
import os
import sys
import numpy as np
import itertools
import fuzzy



sys.stdout = os.fdopen(sys.stdout.fileno(), 'wb', 0)
filenames = [f for idx, f in enumerate(sys.argv) if idx > 0]

innovator_file = filenames[0]
emitter_file = filenames[1]
output_file = filenames[2]

## Assumes that both files have a 'name' and 'id' field
innovators = pd.read_csv(innovator_file)
emitters = pd.read_csv(emitter_file)

def format_name(name):
    name_split = name.lower().split(' ')
    name_sort = sorted(name_split)
    name_out = [word for word in name_sort if len(word) > 1]
    if len(name_out) > 0:
        return ' '.join(name_out)
    else:
        return ' '.join(name_sort)

def block_by_name(names, block_len, how='char'):
    if how == 'char':
        block_str = [n[0:block_len] for n in names]
    if how == 'metaphone':
        dmeta = fuzzy.DMetaphone(block_len)
        block_str = [dmeta(n)[0] for n in names]
        
    return block_str

innovator_names = [format_name(n) for n in innovators['name'].values]
emitter_names = [format_name(n) for n in emitters['name'].values]
innovators['name'] = innovator_names
emitters['name'] = emitter_names

innovator_block = block_by_name(innovators.name, 3, how='metaphone')
emitter_block = block_by_name(emitters.name, 3, how='metaphone')

innovators['block'] = innovator_block
emitters['block'] = emitter_block
innovators.set_index('block', inplace=True)
emitters.set_index('block', inplace=True)
## Take the set intersection; note that this means we don't examine some names...
block_intersect = set(innovator_block).intersection(set(emitter_block))
block_union = set(innovator_block).union(set(emitter_block))



## then compare
def return_match_index(query, possible):
    dist = [normalizedAffineGapDistance(query, p)
            for p in possible
            ]
    match_index = np.argmin(dist)
    return match_index, dist[match_index]

out = []
for idx, block in enumerate(block_intersect):
    print 'Disambiguating block ' + str(idx) + ' of ' + str(len(block_intersect))
    emitter_block = emitters.ix[block]
    innovator_block = innovators.ix[block]
    if len(emitter_block.shape) == 1:
        emitter = emitter_block['name']
        emitter_id = emitter_block['id']
        match_index, match_dist = return_match_index(emitter, innovator_block['name'])
        match_name = innovators.ix[block]['name'][match_index]
        match_idx = innovators.ix[block]['id'][match_index]
        match_pair = (emitter['id'], emitter, match_idx, match_name, match_dist)
        out.append(match_pair)
    else:
        for id, emitter in emitters.ix[block].iterrows():
            match_index, match_dist = return_match_index(emitter['name'], innovator_block['name'])
            match_name = innovators.ix[block]['name'][match_index]
            match_idx = innovators.ix[block]['id'][match_index]
            match_pair = (emitter['id'], emitter['name'], match_idx, match_name, match_dist)
            out.append(match_pair)

df_out = pd.DataFrame(out, columns=['emitter_id', 'emitter_name', 'innovator_id', 'innovator_name', 'dist'])
df_out.to_csv(output_filename, index=False)
        
