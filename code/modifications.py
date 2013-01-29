# Amendments to query_clean
# 29 January 2013



# To clean out  the addresses
# Subset dataframe to only have potential names with address information, strip out address information and attach to 'address' field.


start = time.time()

criterion = fung_sub['person_name'].map(lambda x: x.endswith(' NL'))
has_address = fung_sub[criterion]
has_address = fung_sub[pandas.isnull(fung_sub['person_address'])]
has_address_ids = has_address.index.tolist()
has_address['together'] = [ has_address.person_name.ix[pid].split(',') for pid in ha[criterion]s_address_ids]
has_address['name'] = [ ''.join(has_address.together.ix[pid][0:-2]) for pid in has_address_ids]
has_address['address'] = [ has_address.together.ix[pid][-2:] for pid in has_address_ids]

end = time.time()
runtime = end-start
print runtime



# To clean out the names
# Separate multinames into individuals and 'append' data ?

criterion = fung_sub['person_name'].map(lambda x: len(x)>60)
multi_names = fung_sub[criterion]

def test_length(multiname):
    names = multiname.split(',')
    test_lengths = [len( name.split(' ')) for name in names]
    if 1 in test_lengths:
        names = [multiname]
    
    return names
