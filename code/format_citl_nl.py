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
    str_split = str1.split(' ')
    n_candidates = [word for word in str_split if word in city_list]
    if len(n_candidates) > 0:
        return(n_candidates[0])
    else:
        return None
    
def hashed_geocoder(cities, city_list, lat_list, lng_list, hashfun, jaccard_threshold):
    """
    Given a set of cities to geocode, compares to a master city_list and
    returns a lat, lng pair. If no exact match is found, compares using the jaccard index against a threshold.
    First hashes both the cities and the master cities to permit
    faster lookups in long city lists. 
    """
    hashed_cities = pd.Series([hashfun(c)[0] for c in cities.values])
    hashed_city_list = pd.Series([hashfun(c)[0] for c in city_list.values])
    lat_lng = []
    for n, h in zip(cities.values, hashed_cities.values):
        lat, lng = 0,0
        if h in hashed_city_list.values:
            ## Hash matches
            city_candidates = city_list[hashed_city_list.values == h]
            
            if n in city_candidates.values:
                lat = lat_list[city_list == n].values[0]
                lng = lng_list[city_list == n].values[0]
            else:
                ## Non-exact name match
                n_candidate = string_jaccard(n,
                                             city_candidates.values,
                                             jaccard_threshold
                                             )
                if n_candidate is not None:
                    lat = lat_list[city_list == n_candidate].values[0]
                    lng = lng_list[city_list == n_candidate].values[0]
                else:
                    ## Very inexact match, check if any word matches
                    ## in the city subset
                    n_candidate = search_city(n, city_candidates.values)
                    if n_candidate is not None:
                        lat = lat_list[city_list == n_candidate].values[0]
                        lng = lng_list[city_list == n_candidate].values[0]
                    else:
                        ## Still no match, check every city
                        n_candidate = search_city(n, city_list.values)
                        if n_candidate is not None:
                            lat = lat_list[city_list == n_candidate].values[0]
                            lng = lng_list[city_list == n_candidate].values[0]
        else:
            ## Check every city for word match
            n_candidate = search_city(n, city_list.values)
            
            if n_candidate is not None:
                lat = lat_list[city_list == n_candidate].values[0]
                lng = lng_list[city_list == n_candidate].values[0]
        lat_lng.append((n, lat, lng))
    return lat_lng
                
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
