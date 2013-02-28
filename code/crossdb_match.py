import os
import pandas as pd
import string

os.chdir('/home/Bruegel/patstat_compare')
#os.chdir('/Users/markhuberty/Documents/Research/Papers/psClean/data')


#ehp.to_csv('fung_leuven_han_matchedFEB')
#akzo = ehp[ ehp['fung_name2'] == 'AKZO NOBEL']
#'nl_test_data_12Feb2013.csv'
#neth = pandas.read_csv(fung_input)


def disambig_to_list(disambig_row,
                     delim_primary='###',
                     delim_secondary=','
                     ):
    """
    Takes one row in the fung disambiguator final output, of form
    unique_id###match_prob###match_id,values and returns a
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
    with open(disambig_out, 'r') as infile:
        for row in infile:
            output_element = disambig_to_list(row,
                                              delim_primary,
                                              delim_secondary
                                              )
            df_input.extend(output_element)
    print df_input[0]
    df = pd.DataFrame(df_input, columns=['unique_id', 'orig_id'])
    return df



predisambig_input = 'nl_test_data_12Feb2013.csv'
diambig_results = 'nl_test_output.txt'


fung = disambig_to_df(disambig_results)
neth = pd.read_csv(predisambig_input)

# subset because it is quite a large dataframe
neth = neth[['Person', 'Unique_Record_ID', 'Name']]

fung_map = pd.merge(neth, fung, left_on = 'Unique_Record_ID', right_on = 'orig_id')

#disambigid1, disambigid2 are the ids created in the diambiguation process
fung_map.columns = ['person_id', 'disambigid1', 'name', 'fung_id', 'disambigid2']


leuven11 = pandas.read_csv('leuven2011')
han11 = pandas.read_csv('han2011')

full_map = pandas.merge( leuven11, fung_map, left_on = 'person_id', right_on = 'person')
full_map = pandas.merge( han11, full_map, left_on = 'OCT11_Person_id', right_on = 'person')
