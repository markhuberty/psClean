import pandas as pd
import modifications as md
import fuzzy

city_df = pd.read_csv('/home/markhuberty/Documents/psClean/data/worldcitiespop.txt')
city_df = city_df[city_df.Country=='nl'][['City', 'Latitude', 'Longitude', 'Population']]
is_str = [True if isinstance(city, str) else False for city in city_df.City]
city_df = city_df[is_str]
city_upper = [city.upper() for city in city_df.City]
city_df.City = city_upper

## Hash the city names and group by hash
dmeta = fuzzy.DMetaphone(3)
city_hashes = [dmeta(city)[0] for city in city_df.City]
city_df['city_hash'] = city_hashes

nl_citl = pd.read_csv('/mnt/db_master/patstat_raw/fleming_inputs/citl_netherlands.csv')
nl_citl = nl_citl[['accountholder', 'installationidentifier', 'city']]

nl_citl.city = [city.upper() for city in nl_citl.city]
## Hash the CITL cities too
nl_citl['city_hash'] = [dmeta(city)[0] for city in nl_citl.city]

## then geocode

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

def sort_string(str1):
    str_split = str1.split(' ')
    str_split = sorted(str_split)
    return ' '.join(str_split)

def search_city(str1, city_list):
    """
    Checks whether any words in str1 occur in a list of cities.
    Currently returns just the first match.
    """
    str_split = str1.split(' ')
    n_candidates = [word for word in str_split if word in city_list]
    if len(n_candidates) > 0:
        return(n_candidates[0])
    else:
        return None

def city_checker(n, city_list, lat_list, lng_list, j_threshold):
    """
    Matches a string n to the city list and returns the lat/lng pair.
    Match operates in two stages: check for exact matches, then check for fuzzy
    matches using the Jaccard similarity between strings
    """
    lat_lng = None
    if n in city_list:
        lat_out = lat_list[city_list == n].values[0]
        lng_out = lng_list[city_list == n].values[0]
        lat_lng = (lat_out, lng_out)
    else:
        n_candidate = string_jaccard(n, city_list, j_threshold)

        if n_candidate is not None:
            lat_out = lat_list[city_list == n_candidate].values[0]
            lng_out = lng_list[city_list == n_candidate].values[0]
            lat_lng = (lat_out, lng_out)
    return lat_lng

def hashed_geocoder(cities, city_list, lat_list, lng_list, hashfun, jaccard_threshold):
    """
    Given a set of cities to geocode, compares to a master city_list and
    returns a lat, lng pair. If no exact match is found, compares using the
    jaccard index against a threshold.
    Makes a first pass with hashed values, trying to look at only likely matches in
    long city lists. 
    """
    hashed_cities = pd.Series([hashfun(c)[0] for c in cities.values])
    hashed_city_list = pd.Series([hashfun(c)[0] for c in city_list.values])
    lat_lng_list = []
    for n, h in zip(cities.values, hashed_cities.values):
        lat, lng = 0,0
        if h in hashed_city_list.values:
            ## Hash matches
            city_candidates = city_list[hashed_city_list.values == h]
            lat_lng = city_checker(n,
                                   city_candidates,
                                   lat_list,
                                   lng_list,
                                   jaccard_threshold
                                   )
        else:
            lat_lng = city_checker(n,
                                   city_list,
                                   lat_list,
                                   lng_list,
                                   jaccard_threshold
                                   )

        lat_lng_list((n, lat, lng))
    return lat_lng_list
                
latlng = hashed_geocoder(nl_citl.city,
                         city_df.City,
                         city_df.Latitude,
                         city_df.Longitude,
                         dmeta,
                         0.95
                         )


latlng_df = pd.DataFrame(latlng, columns=['name', 'lat', 'lng'])

nl_citl['Lat'] = latlng_df.lat
nl_citl['Lng'] = latlng_df.lng

nl_citl.to_csv('/mnt/db_master/patstat_raw/fleming_inputs/nl_citl_geocoded.csv')
