# Amendments to query_clean
# 30 January 2013


## To clean out multiple names
# Input: name dataframe
# Return: dataframe with multiple names removes, split, and stacked at the bottom
import time
import pandas as pd
import re

def str_findall(strng, val):
    idx = [m.start() for m in re.finditer(val, strng)]
    return idx

def do_all(fullframe):
    """ 
    Takes in dataframe with name information;
    splits names for rows with multiple name in person field,
    appends each name to auxillary information,
    drops rows with multiple names; appends rows of split names 
    
    """
    
    start = time.time()
   
    allcolnames = fullframe.columns.tolist()
    allcolnames.pop(allcolnames.index('Name'))
    colnames  = allcolnames
    criterion = fullframe['Name'].map(lambda x: len(x)>60)
    multinames = fullframe[criterion]
    max_id = max(fullframe.Unique_Record_ID)
    splitnames = get_multi_names(multinames, colnames, max_id) 
    
    name1 = fullframe.drop(multinames.index.tolist())
    mynames = name1.append(splitnames)

    end = time.time()
    runtime = end-start
    print runtime

    return mynames


print 'me'
def split_multi_names(multiname):
    """get list of separated names """
    names = multiname.split(',')
    check_lengths = [len( name.split(' ')) for name in names]
    if 1 in check_lengths:
        names = [multiname]
    return names

def get_multi_names(myframe, colnames, max_id):
    """ get data frame of split names with associated information """
    all_names = list()    
    for idx in myframe.index.tolist():
        dd = dict( [ (col, myframe.get_value(idx, col))
                     for col in colnames] )
        names = split_multi_names(myframe.get_value(idx, 'Name'))
        data = [ dict( dd.items()+ [('Name', name)]) for name in names]
    
        all_names += data
    
    split_names = pd.DataFrame(all_names)
    max_unique_id = max(myframe.Unique_Record_ID)
    new_unique_ids = range(max_id + 1, max_id + 1 + split_names.shape[0])
    split_names['Unique_Record_ID'] = new_unique_ids
    return split_names
         


#To clean out address inforamation - find records where person name has country code 
#    and there is no information in address field.

def do_addresses(myframe, country_code):
    """ 
    Takes in dataframe, replaces adress names with cleaned name, 
    inserts address information to address field.
    Returns updated dataframe.

    """
    
    fung_edit = myframe[['Name', 'Address']]
    fung_stay = myframe[['Patent', 'Person','Country', 'LegalId', 'Coauthor', 'Class', 'Year', 'Lat', 'Lng', 'Locality', 'Unique_Record_ID']]


    start = time.time()

    criterion = myframe['Name'].map(lambda x: x.endswith(' ' + country_code))
    has_address = myframe[criterion]
    has_address = has_address[pd.isnull(has_address['Address'])]
    has_address_idx = has_address.index.tolist()
    
    comma_idx = [str_findall(n, 'country_code') for n in has_address.Name.values]

    names = []
    addresses = []
    for idx, comma_loc in enumerate(comma_idx):
        if len(comma_loc) >= 2:
            this_field = has_address.Name.values[idx]
            this_name = this_field[0:comma_loc[-2]]
            this_address = this_field[comma_loc[-2]:]
            names.append(this_name)
            addresses.append(this_address)
        else:
            names.append(has_address.Name.values[idx])
            addresses.append(has_address.Address.values[idx])

    
    fung_edit.ix[has_address_idx, "Name"] = names
    fung_edit.ix[has_address_idx, "Address"] = addresses
    myframe = pd.merge(fung_stay, fung_edit, left_index = True, right_index = True)

    end = time.time()
    runtime = end-start

    print runtime
    return myframe


## Two address approaches:
## 1. Look for double occurrances of country code
## 2. Look for names that end in \scountry_code,
##    hack off everything from prior comma
## 3. Look for city in the name string, take the last city instance
def find_address(names, country_code, cities):

    #addr_regex = re.compile(country_code + '[\w\s,&]+?' + country_code + '$')
    addr_regex = re.compile('\s[0-9]+?[\w\s,&]+?' + country_code + '$')
    #addr_regex = re.compile('basdfa')

    names_clean = []
    addresses_clean = []
    lat_list = []
    lng_list = []
    locality_list = []
    addr_indices = []
    counter = 0
    
    for idx, n in enumerate(names):
        this_name = n
        this_address = ''
        locality = ''
        lat = 0
        lng = 0
        r_addr = find_address_regex(n, addr_regex)

        if r_addr:
            this_name = n[0:r_addr.start()]
            this_address = r_addr.group(0)
            locality, lat, lng = geocode_from_city_name(this_address, cities)
            addr_indices.append(idx)
            counter += 1
        elif n.endswith(' ' + country_code):
            c_addr = find_address_comma(n, min_len=60)
            if c_addr:
                this_name = c_addr[0]
                this_address = c_addr[1]
                locality, lat, lng = geocode_from_city_name(this_address, cities)
                addr_indices.append(idx)
                counter +=1
            else:
                city_addr = find_address_city(n, cities)
                if city_addr:
                    this_name = city_addr[0]
                    this_address = city_addr[1]
                    locality, lat, lng = geocode_from_city_name(this_address, cities)
                    addr_indices.append(idx)
                    counter += 1
        names_clean.append(this_name)
        addresses_clean.append(this_address)
        lat_list.append(lat)
        lng_list.append(lng)
        locality_list.append(locality)

    print counter
    return names_clean, addresses_clean, lat_list, lng_list, locality_list


def find_address_regex(n, addr_regex):
    out = addr_regex.search(n)
    return out

def find_address_comma(n, min_len=60):
    """
    Checks whether we have address-like information
    on the supposition that addresses for strings longer
    thank min_len start at the second-to-last comma
    """
    out = None
    if len(n) > min_len:
        comma_loc = str_findall(n, ',')
        if len(comma_loc) > 2:
            n_out = n[0:comma_loc[-2]]
            addr = n[comma_loc[-2]:]
            out = (n_out, addr)
    return out

def find_address_city(n, cities):
    """
    Checks for city-only address information by checking each
    word in an address against a city list. 'cities' should be either
    a list or a dict w/ cities as keys; the dict is better as lookup scales.
    """
    out = None
    n_words = n.split(' ')

    city_search = [n_words.index(word) for word in n_words if
                   word in cities
                   ]
    if len(city_search) > 0:
        city_index = max(city_search)
        this_name = ' '.join(n_words[0:city_index])
        this_address = ' '.join(n_words[city_index:])
        out = (this_name, this_address)
    return out

def geocode_from_city_name(address, city_lat_lng):
    """
    Given an address and a dict of city:(lat, lng),
    return the city and the geocoordinates if the city name occurs
    in the address. 
    """
    out = None
    address_words = address.split(' ')

    city_search = [word for word in address_words if word in city_lat_lng]

    if len(city_search) > 0:
        city = city_search[-1]
        lat_lng = city_lat_lng[city]
        out = (city, lat_lng[0], lat_lng[1])
    else:
        out = ('NONE', 0, 0)
    return out


def sort_name(name_string, threshold=1):
    """
    sorts a name string alphabetically by word, keeping only words
    longer than threshold characters
    """
    name_split = name_string.split(' ')
    name_long = [n for n in name_split if len(n) > threshold]
    if len(name_long) > 0:
        name_sorted = sorted(name_long)
        out = ' '.join(name_sorted)
        out = clean_name(out)
    else:
        out = ''
    return out

def clean_name(name_string, regex_string='[,\.]'):
    """
    Strips characters from a string; should be used for punctuation cleaning;
    returns string w/o leading, trailing spaces
    """
    out = re.sub(regex_string, '', name_string)
    out = out.strip()
    return out
