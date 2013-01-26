import os
import pandas as pd
import string
import time

def cosine_sim(set1, set2):
    numerator = len(set1 & set2)
    denom = len(set1.union(set2))
    return numerator / float(denom)

def pds_block(ids, names, n_chars):
    block_val = [n[0:n_chars] for n in names]
    df = pd.DataFrame({'idx': ids,
                       'name': names,
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
    print 'Set time= %s' % set_duration
    
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
            else:
                continue
            counter += 1
            if counter % 10000 == 0:
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
nl_potential_flags = flag_errors_blocked(fung_sub.unique_id,
                                         fung_sub.name,
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

