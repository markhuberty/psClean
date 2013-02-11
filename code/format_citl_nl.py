import pandas as pd
import modifications as md
import fuzzy


ctry_codes = ['NG', 'KY', 'CN', 'LV', 'CZ', 'ES', 'HU','CH', 'MU',
              'BE', 'IT', 'LB', 'KZ', 'IN', 'KR', 'HR', 'CY', 'BM',
              'AT', 'NL', 'PL', 'AZ', 'EE', 'PT', 'TH', 'RU', 'RS',
              'UA', 'FI', 'NZ', 'SE', 'TN', 'LT', 'MT', 'CL', 'VG',
              'QA', 'BY', 'MC', 'LI', 'SG', 'GB', 'TW', 'BG', 'MG',
              'SI', 'LK', 'JE', 'BN', 'BH', 'BR', 'GR', 'IS', 'EG',
              'ME', 'SK', 'NO', 'IL', 'LU', 'KW', 'CA', 'TR', 'SC',
              'ZA', 'JO', 'MA', 'BS', 'JP', 'DK', 'AE', 'DE', 'VN',
              'IM', 'GE', 'RO', 'AU', 'US', 'SA', 'DZ', 'HK', 'MX',
              'IE', 'FR']

name_in = '/home/ammaserwaah/citl_data/countries/%scode.csv'
name_out = '/home/ammaserwaah/citl_data/geocoded/%s_citl.csv'

city_df = pd.read_csv('/home/markhuberty/Documents/psClean/data/worldcitiespop.txt')
city_df = city_df[['City', 'Latitude', 'Longitude', 'Population', 'Country']]
is_str = [True if isinstance(city, str) else False for city in city_df.City]
city_df = city_df[is_str]
city_upper = [city.upper() for city in city_df.City]
city_df.City = city_upper

## Hash the city names and group by hash
dmeta = fuzzy.DMetaphone(3)
city_hashes = [dmeta(city)[0] for city in city_df.City]
city_df['city_hash'] = city_hashes



## then geocode

def string_jaccard(str1, strlist, threshold=0.8):
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

def geocode_country(ctry_citl, city_df, name_out):
                
    latlng = hashed_geocoder(ctry_citl.city,
                             city_df.City,
                             city_df.Latitude,
                             city_df.Longitude,
                             dmeta,
                             0.8
                         )

    latlng_df = pd.DataFrame(latlng, columns=['name', 'lat', 'lng'])

    ctry_citl['Lat'] = latlng_df.lat
    ctry_citl['Lng'] = latlng_df.lng

    ctry_citl.to_csv(name_out, encoding = 'utf8')

    return None



def country_specific(city_df, ccode, name_in, name_out):
    name_in = name_in % ccode
    name_out = name_out % ccode
    
    ctry_df = city_df[city_df.Country==ccode.lower()]
 
    ctry_citl = pd.read_csv(name_in)
    ctry_citl = ctry_citl[['accountholder', 'installationidentifier', 'city']]
    ctry_citl.city = [city.upper() for city in ctry_citl.city]
    ctry_citl['city_hash'] = [dmeta(city)[0] for city in ctry_citl.city]
                              
    geocode_country(ctry_citl, ctry_df, name_out)
                              
    return None

mytest = ['NL']
geocoded = [ country_specific(city_df, ccode, name_in, name_out) for ccode in ctry_codes]

print 'done!'
