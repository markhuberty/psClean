# Amendments to query_clean
# 30 January 2013


## To clean out multiple names
# Input: name dataframe
# Return: dataframe with multiple names removes, split, and stacked at the bottom

def do_all(fullframe):
    """ 
    Takes in dataframe with name information;
    splits names for rows with multiple name in person field,
    appends each name to auxillary information,
    drops rows with multiple names; appends rows of split names 
    
    """
    
    start = time.time()
   
    colnames = ['leuven_id', 'fung_name']
    criterion = fullframe['person_name'].map(lambda x: len(x)>60)
    multinames = fullframe[criterion]

    splitnames = get_multi_names(multinames) 
    
    name1 = fullframe.drop(multinames.index.tolist())
    mynames = name1.append(splitnames)

    end = time.time()
    runtime = end-start
    print runtime

    return mynames


def split_multi_names(multiname):
    """get list of separated names """
    names = multiname.split(',')
    check_lengths = [len( name.split(' ')) for name in names]
    if 1 in check_lengths:
        names = [multiname]
    return names

def get_multi_names(myframe):
    """ get data frame of split names with associated information """
    all_names = list()    
    for idx in myframe.index.tolist():
        dd = dict( [ (col, myframe.get_value(idx, col))
                     for col in colnames] )
        names = split_multi_names(myframe.get_value(idx, 'person_name'))
        data = [ dict( dd.items()+ [('person_name', name)]) for name in names]
    
        all_names += data
    split_names = pandas.DataFrame(all_names)
    return split_names
         


#To clean out address inforamation - find records where person name has country code 
#    and there is no information in address field.

def do_addresses(myframe):
    """ 
    Takes in dataframe, replaces adress names with cleaned name, 
    inserts address information to address field.
    Returns updated dataframe.

    """
    
    fung_edit = myframe[['person_name', 'person_address']]
    fung_stay = myframe[['leuven_name', 'leuven_id', 'leuven_hrm_level', 
                         'person_ctry_code', 'sector', 'fung_id', 
                         'fung_name', 'Apr12_Person_id', 'han_id', 
                         'han_name', 'TRUEfung_id', 'has_fung', 'has_han']]

    start = time.time()

    criterion = myframe['person_name'].map(lambda x: x.endswith(' NL'))
    has_address = myframe[criterion]
    has_address = has_address[pandas.isnull(has_address['person_address'])]
    
    ids = has_address.index.tolist()
    together = [ has_address.person_name.ix[pid].split(',') for pid in ids]


    names = [ (''.join(thing[0:-2]), thing[-2:]) for thing in together]
    fung_edit.ix[ids] = names 
    
    myframe = pandas.merge(fung_stay, fung_edit, left_index = True, right_index = True)

    end = time.time()
    runtime = end-start

    print runtime
    return myframe
