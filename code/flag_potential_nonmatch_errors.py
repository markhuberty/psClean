import os
import pandas as pd
import string
import time

def cosine_sim(set1, set2):
    numerator = len(set1 & set2)
    denom = len(set1.union(set2))
    if denom > 0:
        return numerator / float(denom)
    else:
        return 0

def pds_block(ids, names, n_chars):
    block_val = [n[0:n_chars] for n in names]
    df = pd.DataFrame({'idx': ids.values,
                       'name': names.values,
                       'block': block_val}
                      )
    df_grouped = df.groupby('block')
    return df_grouped

def flag_errors_blocked(ids, names, n_chars, threshold, test_threshold):
    df_blocked = pds_block(ids, names, n_chars)

    out = {}
    for blockname, block in df_blocked:
        flagged = flag_errors(block.idx,
                              block.name,
                              threshold
                              )
        if len(flagged) > 0:
            out_df = pd.DataFrame(flagged, columns=['id1', 'name1', 'id2', 'name2'])
            out[blockname] = out_df
        if test_threshold is not None and len(out) > test_threshold:
            break
    return out

def flag_errors(ids, names, threshold):
    """
    Given a set of names, do the pairwise compare and flag
    very similar ones. Uses cosine string similarity
    """

    ## precompute the sets
    set_start = time.time()
    sets = [set(n) for n in names]
    set_end = time.time()
    set_duration = (set_end - set_start) / len(names)
    # print 'Set time= %s' % set_duration
    
    out = []
    counter = 0
    start_time = time.time()
    for i in range(len(ids)):
        for j in range(i + 1, len(ids)):
            s1 = sets[i]
            s2 = sets[j]
            name1 = names.values[i]
            name2 = names.values[j]

            dist = cosine_sim(s1, s2)

            if dist > threshold:
                out.append((ids.values[i], name1,
                            ids.values[j], name2
                            )
                           )

            counter += 1
            if (counter % 10000) == 0:
                print counter
                this_time = time.time()
                txn_time = (this_time - start_time) / counter
                print txn_time

    return out

def disambig_to_list(disambig_row,
                     delim_primary='###',
                     delim_secondary=','
                     ):
    """
    Takes one row in the fung disambiguator final output, of form
    unique_id###match_prob###match,id,values and returns a
    list of form [(unique_id, match_id), ]
    """
    primary_split = disambig_row.split(delim_primary)
    matching_ids = primary_split[2].rstrip(',\n').split(delim_secondary)
    input_list = [(int(primary_split[0]), int(mid)) for mid in matching_ids]

    return input_list

def disambig_to_df(disambig_out,
                   delim_primary='###',
                   delim_secondary=','
                   ):
    """
    Reformats the fung disambuguator output to a python
    data frame of form unique_id:match_id
    
    """
    df_input = []
    with open(disambig_out, 'rt') as infile:
        for row in infile:
            output_element = disambig_to_list(row,
                                              delim_primary,
                                              delim_secondary
                                              )
            df_input.extend(output_element)
    print df_input[0]
    df = pd.DataFrame(df_input, columns=['unique_id', 'orig_id'])
    return df


os.chdir('/Users/markhuberty/Documents/Research/Papers/psClean/data')


## Then load it
fung_orig = pd.read_csv('nl_test_data_sub.csv',
                        header=None,
                        names=['unique_id', 'person_id', 'name']
                        )

## Then get just the unique IDs
fung_match = disambig_to_df('final.txt')
unique_ids = fung_match.unique_id.drop_duplicates()

## And subset the names to just hte uniques
bool_idx = fung_orig.unique_id.isin(fung_match.unique_id)

fung_sub = fung_orig[bool_idx]

## Issues here:
## 1. What about first-letter misspellings, name reversals, etc
## 2. Will ID stuff w/ separate names that aren't

## Now resort names
def sort_name(name_instance, min_length):
    name_split = name_instance.split()
    name_sort = sorted([n for n in name_split if len(n) >= min_length ])
    name_out = ' '.join(name_sort)
    return name_out

def sort_names(names, min_length=3):
    out = [sort_name(n, min_length) for n in names]
    return pd.Series(out)

sorted_names = sort_names(fung_sub.name, min_length=2)
    
nl_potential_flags = flag_errors_blocked(fung_sub.unique_id,
                                         sorted_names,
                                         3,
                                         0.98,
                                         test_threshold=None
                                         )

block_counts = [len(nl_potential_flags[nlf]) for nlf in nl_potential_flags]
counts = np.sum(block_counts)

## Write it out
for idx, k in enumerate(nl_potential_flags.keys()):
    if idx == 0:
        df = nl_potential_flags[k]
    else:
        df = df.append(nl_potential_flags[k], ignore_index=True)

df.to_csv('nl_candidate_false_nonmatches.csv',
          index=False
          )


## Some experiments w/ metaphone hashing
import fuzzy
import math
dhash = fuzzy.DMetaphone(3)
df_hash = nl_potential_flags[nl_potential_flags.keys()[-1]]

def hash_names(ids, names, hashfun, num_hashes = 3):
    hashes = []
    for idx1, n in zip(ids.values, names.values):
        n_split = [ns for ns in n.split() if len(ns) > 2]
        hashlist = sorted([hashfun(ns)[0] for ns in n_split])
        hash_out = [None] * num_hashes
        for idx2, h in enumerate(hashlist):
            if idx2 < num_hashes:
                hash_out[idx2] = h
            else:
                break
        this_hash = [idx1]
        this_hash.extend(hash_out)
        hashes.append(this_hash)
    return hashes

#test_hash = hash_names(fung_sub.unique_id, fung_sub.name, dhash, 3)

def stack_groups(grouplist):
    for idx, group in enumerate(grouplist):
        if idx == 0:
            df = group
        else:
            df = df.append(group, ignore_index=True)
    return df

## Returns the indices only
def check_flag_dim(df, num_hashes):
    ct_threshold = math.ceil(num_hashes / float(2.0))
    grouped = df.groupby(['name1', 'name2'])
    gsize = grouped.size()
    #gsize = gsize[gsize >= ct_threshold]
    return gsize


def multihash_flagger(ids, names, hashfun, num_hashes, sim_threshold):
    name_hashes = hash_names(ids, names, hashfun, num_hashes)
    colnames = ['idx']
    hash_colnames = ['hash' + str(num) for num in range(num_hashes)]
    colnames.extend(hash_colnames)
    print 'building hash dataframe'
    name_hashes = pd.DataFrame(name_hashes, columns=colnames)
    print name_hashes.shape
    df = pd.DataFrame({'idx': ids, 'name':names})
    all_groups = []
    for col in hash_colnames:
        print 'grouping names'
        names_grouped = df.groupby(name_hashes[col])
        print 'flagging errors'
        flag_groups = []
        for groupname, group in names_grouped:
            flags = flag_errors(group.idx, group.name, sim_threshold)
            if len(flags) > 0:
                df_flags = pd.DataFrame(flags, columns=['id1', 'name1', 'id2', 'name2'])
                df_flags['hashname'] = groupname
                print df_flags.shape
                flag_groups.append(df_flags)
            else:
                continue

        print 'stacking flag groups'
        print len(flag_groups)
        stacked_groups = stack_groups(flag_groups)
        print 'appending stacked groups'
        all_groups.append(stacked_groups)
    print 'hashset length'
    print len(all_groups)
    for g in all_groups:
        print g.shape
    df_all = stack_groups(all_groups)
    print 'output shape'
    print(df_all.shape)
    
    valid_pairs = check_flag_dim(df_all, num_hashes)

    return valid_pairs, df_all

test_hash, test_df = multihash_flagger(fung_sub.unique_id,
                                       fung_sub.name,
                                       dhash,
                                       5,
                                       0.95
                                       )

name_hashes = hash_names(fung_sub.unique_id,
                         fung_sub.name,
                         dhash,
                         3
                         )

df = pd.DataFrame(name_hashes, columns=['idx', 'h1', 'h2', 'h3'])

df2 = fung_sub[['unique_id', 'name']]
df2.columns=['idx', 'name']
grouped = df2.groupby(df['h1'])

out = {}
for groupname, group in grouped:
    flags = flag_errors(group.idx, group.name, 0.98)
    if len(flags) > 0:
        out[groupname] = flags


## What if we just resort the names and then block?
