import pandas
import os
import numpy
import random
import code
from collections import namedtuple
from itertools import chain


## Generate comparitave statistics for fung, leuven, han disambiguation results


os.chdir('/home/Bruegel/patstat_compare')


# ## Load and subset appropriate dataframes
# enhanced_person = pandas.read_csv('neth_matched_data', index_col = 'person_id')
# enhanced_person['has_fung'] = enhanced_person['fung_id'].notnull()
# enhanced_person['has_han'] = enhanced_person['han_id'].notnull()

# fung_sub = enhanced_person[enhanced_person['has_fung'] == True]
# han_sub = enhanced_person[enhanced_person['has_han'] == True]

# leuven_l1 = fung_sub[fung_sub['leuven_hrm_level'] != 2]
# leuven_l2 = fung_sub[fung_sub['leuven_hrm_level'] == 2]


# subsets = {'fung': (fung_sub, 0), 'leuven': (fung_sub, 1), 'leuven_l1': (leuven_l1, 1), 'leuven_l2': (leuven_l2, 1)}
# groupby_tags = ['fung', 'leuven']

# Gnerate comparative statistics for each grouping

mydata = namedtuple('mydata', 'nuniques average max overaggregate aggregate_count counts duplicates grouping')
data_out = pandas.DataFrame(index = subsets.keys(), 
                            columns = ['n_uniques', 'max', 'min', 'overaggregate', 'agg_count'])


def get_important_stats(subset, groupid, otherid):
    
    grouping = subset.groupby(groupid)
    
    counts = {'counts':grouping.size(), 
              'uniques': pandas.Series(index = grouping.groups.keys())}
    
    unique_counts = list()
    non_consolidated = list()
    

    for gid, group in grouping:
        consolidate = set(group[otherid])
        n = len(consolidate)
        counts['uniques'][gid] = n

        if n>1:
            non_consolidated.append(gid)
    
    n_uniques = len(counts['counts'])    
    all_counts = pandas.DataFrame(counts)
        
    maximum = max(all_counts['counts'])
    average = numpy.mean(all_counts['counts'])
    over = len(non_consolidated)
    average_non_consolidated = numpy.mean(all_counts['uniques'])

    return mydata(n_uniques, maximum, average, over, average_non_consolidated, all_counts, non_consolidated, grouping)


def build_stats_dic():

    for key, value in subsets.items():
        subset = value[0]
        groupid = groupby_tags[value[1]] + '_id'
        otherid = groupby_tags[value[1]-1] + '_id'
    
        stats = get_important_stats(subset, groupid, otherid) 

        subsets[key] = stats
        data_out.ix[key] = subsets[key][0:5]
    return subsets


# Write out data to latex table

def write_to_table(mydataframe, name):
    with open(name, 'w') as myfile:
        myfile.write(mydataframe.to_latex())
    return None


# View some specific samples:

def random_sampling(k, pids_list, subcat):
    """ random sampling of duplicate fung_ids"""
    lids = [ random.choice(pids_list) for i in range(k)]
    l2_pids = [ subsets[subcat].grouping.groups[lid] for lid in lids]
    pids = list(chain.from_iterable(l2_pids))
        
    sample = fung_sub.ix[pids][['person_name', 'fung_id', 'leuven_id']]
    return sample


def random_aggregates_sampling(subset):
    """function to sample where fung has overaggregated"""
    grouping = subsets[subset].grouping
    duplicates = subsets[subset].duplicates
    
    for i in range(100):
        value = random.choice(duplicates)
        duplicates.remove(value)
        pids = grouping.groups[value]
        
        sample = fung_sub.ix[pids][['person_name', 'fung_id', 'leuven_id']]

        name = 'fung_overaggregate'
        sample.to_csv(name, mode = 'a')
        write_to_table(sample, name)
        
    return None


# Get additional data from original PATSAT

def get_pids():
    """specify the types of person_id's"""
    all_pids = fung_sub.index.tolist()

    lid1 = subsets['leuven_l1'].grouping.groups
    lid2 = subsets['leuven_l2'].grouping.groups
    
    leuven1_ids = [lid1[lid] for lid in subsets['leuven_l1'].duplicates]    
    leuven2_ids = [lid2[lid] for lid in subsets['leuven_l2'].duplicates]
    
    l1_pids = list(chain.from_iterable(leuven1_ids))
    l2_pids = list(chain.from_iterable(leuven2_ids))
    
    non_dup_pids = list( set(all_pids).difference(set(l1_pids+l2_pids)))
    
    print len(non_dup_pids)
    pids = (l1_pids, l2_pids, non_dup_pids)

    return pids



def get_additional_data(pids_list):
    """get additional data from original files,
    read data in for specific subset of person_ids (e.g. duplicates etc)
    either drop duplicate_person ids for small file size, or keep to count total patents"""

    fungiterator = pandas.read_csv('nl_test_data.csv', chunksize = 100000)
    catchframe = pandas.DataFrame()
    for chunk in fungiterator:
        chunk['person_id'] = chunk['Person']
        chunk = chunk.set_index('Person')
        pid_data = chunk.ix[pids_list]
        if len(pid_data) > 1:
            catchframe = pandas.concat([catchframe, pid_data])

    grouping = catchframe.groupby('person_id')
    mean_patents = numpy.mean(grouping.size())

    catchframe = catchframe.drop_duplicates('person_id')
    catchframe['class_ct'] = [ len(catchframe.get_value(pid, 'Class').split('**')) for pid in pids_list ]                
    catchframe['coauth_ct'] = [ len(catchframe.get_value(pid, 'Coauthor').split('**')) for pid in pids_list ]                
    
    mean_class = numpy.mean(catchframe['class_ct'])
    mean_coauth = numpy.mean(catchframe['coauth_ct'])

    augmented_data = catchframe.join(fung_sub)
    augmented_data = augmented_data[['person_name', 'fung_id', 'leuven_id', 'coauth_ct', 'class_ct']]

    return (augmented_data, mean_patents, mean_class, mean_coauth)



def get_augmented_stats():

    pids = get_pids()
    methods = { 'leuven1': pids[0], 'leuven2': pids[1], 'nodups': pids[2]}
    data_out2 = pandas.DataFrame(index = methods.keys(), 
                            columns = ['avg_patents', 'avg_ipc', 'avg_coauths'])

    for method, ids in methods.items():
        results = get_additional_data(ids)
        data_out2.ix[method] = results[1:]
    
    return data_out2



# Estimating precision 

problem_ids = [149952, 584765, 414675, 273757, 368190, 194155, 137027, 181544, 48698, 245115, 457476, 455839, 366609, 365998, 92850, 67917, 976231, 315889, 143299, 369568]

def check_problems():
    for i in problem_ids:
        print fung_sub[fung_sub['fung_id'] == i][['person_name', 'leuven_name', 'fung_name']].to_string()
        agreement = raw_input('Do you agree? yes, no')
        if agreement == 'n':
            problem_ids.remove(i)
    return None
 

print 'hello world!'

