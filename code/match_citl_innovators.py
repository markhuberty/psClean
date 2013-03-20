import csv
from dedupe.affinegap import normalizedAffineGapDistance
import os
import sys
import numpy as np
import itertools



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

def block_by_name(names, block_len):
    block_str = [n[0:block_len] for n in names]
    return block_str


innovators['name'] = [format_name(n) for n in innovators['name']]
emitters['name'] = [forname_name(n) for n in innovators['name']]

innovator_block = block_by_name(innovators.name, 3)
emitters_block = block_by_name(emitters.name, 3)

## Take the set intersection; note that this means we don't examine some names...
block_intersect = set(innovators_block).intersection(set(emitters_block))
block_union = set(innovators_block).union(set(emitters_block))


## Walk across the blocks and match

## First group data by block
grouped_innovators = innovators.groupby(innovators_block)
grouped_emitters = emitters.groupby(emitters_block)

## then compare
out = []
for idx, block in enumerate(block_intersect):
    if block in innovator_block:
        print 'Disambiguating block ' + str(idx) + ' of ' + str(len(blocks))
        for id, emitter in itertools.izip(grouped_emitters.groups[block].id, grouped_emitters.groups[block].name):
            dist = [normalizedAffineGapDistance(emitter, innovator) for innovator in grouped_innovators.groups[block].name]
            match_index = np.argmin(dist)
            match_name = grouped_innovators.groups[block].name[match_index]
            match_idx = grouped_innovators.groups[block].id[match_index]
            match_pair = (id, emitter, match_idx, match_name, dist)
            out.append(match_pair)

df_out = pd.DataFrame(out, columns=['emitter_id', 'emitter_name', 'innovator_id', 'innovator_name', 'dist'])
df_out.to_csv(output_filename, index=False)
        
