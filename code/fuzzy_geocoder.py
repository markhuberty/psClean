# fuzzy_geocoder.py
# Author: Mark Huberty
# Begun: 18 April 2013
# Purpose: given a list of addresses and dict of possible cities for those addresses,
# geocode at the city level for those addresses
# Does both exact and fuzzy checks for city matches to handle misspellings.
# The fuzzy matches require a hash function to hash the city names first, so that
# the comparison set is smaller. 

import pandas as pd
import Levenshtein
import re

def address_geocoder(addresses, city_df, threshold, hashfun):
    city_df.city_hash = [hashfun(c)[0] for c in city_df.cities]
    geocoded_addresses = [city_geocoder(a, city_df, hashfun, threshold) for a in addresses]


def city_geocoder(addr, city_df, hashfun, threshold):
    # Assumes city_df is a pandas data_frame with city, city_hash, lat, lng
    addr_words = addr.split(' ')

    city_latlng = simple_address_check(addr_words, city_df)
    if city_latlng[0] is None:
        city_latlng = fuzzy_address_check(addr_words, city_df, hashfun, threshold)
    return city_latlng


def simple_address_check(addr, city_df):
    city_df.set_index('city', inplace=True) ## is this going to modify the global?
    city_candidates = []
    for w in addr.reverse():
        try:
            city = city_idf[w]
        except IndexError:
            continue
        city_candidates.append((city,
                                city_df[w].lat,
                                city_df[w].lng
                                )
                               )
    if len(city_candidates) > 0:
        return city_candidates[-1]
    else:
        return [None] * 3


def fuzzy_address_check(addr, city_df, hashfun, j_threshold):
    city_df.set_index('city_hash', inplace=True)
    hashed_addr = [hashfun(w)[0] for w in addr]
    hashzip = zip(addr, hashed_addr)
    city_candidates = []
    for w, h in hashzip.reverse():
        try:
            sub_df = city_df[h]
        except IndexError:
            continue
        city = string_jaccard(w, sub_df.city.values, J_threshold)
        city_candidates.append((city,
                                city_df.lat[city],
                                city_df.lng[city]
                                )
                               )
    if len(city_candidates) > 0:
        return city_candidates[0]
    else:
        return [None] * 3



def fuzzy_city_check(addr, city_df, hashfun, j_threshold):
    addr = re.sub('^[0-9]+|[0-9]+$', '', addr)
    split_addr = re.split('[0-9]+', addr)
    city_chunk = re.sub('[0-9]+', '', split_addr[-1]).strip()
    addr_candidates = city_chunk.split(' ')
    hashed_addr = [hashfun(w)[0] for w in addr_candidates]
    print hashed_addr
    sub_df = city_df[city_df.city_hash.isin(hashed_addr)]
    print sub_df.shape

    if sub_df.shape[0] > 0:
        city_match = search_city(sub_df.city.values,
                                 city_chunk,
                                 j_threshold
                                 )

        if city_match:
            out = [city_match,
                   city_df.lat[city_df.city==city_match].values[0],
                   city_df.lng[city_df.city==city_match].values[0]
                   ]
        else:
            out = [None] * 3
    else:
        out = [None] * 3
    return out 

def search_city(cities, addr_string, threshold):
    sims = [Levenshtein.ratio(addr_string, c) for c in cities]
    sorted_cities = zip(sims, cities)
    sorted_cities.sort()
    if sorted_cities[-1][0] > threshold:
        print sorted_cities[-1]
        return sorted_cities[-1][1]
    else:
        return None
    

def string_jaccard(str1, strlist, threshold=0.95):
    """
    Given a string and a list of strings, calculates
    the pairwise jaccard similarity str1:strlist and
    returns the closest for str1 in strlist.
    """
    set1 = set(list(str1))
    setlist = [list(s) for s in strlist]

    jaccards = [jaccard(set1, set2) for set2 in setlist]
    
    sorted_sets = zip(jaccards, strlist)
    sorted_sets.sort()
    # print sorted_sets
    if sorted_sets[-1][0] > threshold:
        out = sorted_sets[-1][1]
    else:
        out = None
    return out


def jaccard(set_a, set_b):
    numer = len(set_a.intersection(set_b))
    denom = len(set_a.union(set_b))
    out = numer / float(denom)
    return out
