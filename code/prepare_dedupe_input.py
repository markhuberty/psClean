import AsciiDammit
import consolidate_df as cd
import modifications as md
import numpy as np
import os
import pandas as pd
import re
import time
import csv
import fuzzy_geocoder
import fuzzy
import name_address_parser as nap

def nan_helper(val):
    try:
        out = np.isnan(val)
    except TypeError:
        out = False
    return out

source_dir = '/mnt/db_master/patstat_raw/fleming_inputs/'
re_file = re.compile('cleaned_output_[A-Z]{2}.tsv$')
file_list = os.listdir(source_dir)
files = [f for f in file_list if re_file.match(f)]
file_header = ['', 'appln_id','person_id','person_name','person_address','person_ctry_code','firm_legal_id','coauthors','ipc_code','year']
dtypes = [np.int32, np.int32, np.int32, object, object, object, object, object, object, np.int32]
typedict = dict(zip(file_header, dtypes))

city_latlong = pd.read_csv('city_latlong.csv')
city_latlong.columns = ['country', 'city', 'lat', 'lng']

dmeta = fuzzy.DMetaphone(3)
city_latlong['city_hash'] = [dmeta(city)[0] for city in city_latlong.city]

    
for f in files:
    print f
    f_in = source_dir + f
    df = pd.read_csv(f_in, sep='\t', dtype=typedict)
    column_check = set(df.columns) & set(file_header)
    if len(column_check) == 0:
        df = pd.read_csv(f_in,
                         sep='\t',
                         header=None,
                         names=file_header,
                         dtype=typedict
                         )

    ## Skip countries with only one record, no dedupe needed
    if len(df.shape) == 1 or df.shape[0] in [0, 1]:
        print 'Skipping ' + f
        continue
    
    df.fillna('', inplace=True)
 
    person_patent_map = df[['person_id', 'appln_id']].drop_duplicates()
    person_patent_map.columns = ['Person', 'Patent']
    
    country = df.person_ctry_code.values[0].lower()
    country_latlong = city_latlong[city_latlong.country == country]
    if country_latlong.shape[0] > 0:
        geocode=True
    else:
        geocode=False

    # Clean up the names and ascii-ize them
    # First clean up names and find addresses
    ascii_names = md.asciidammit(df['person_name'])

    # Then lowercase everything
    ascii_names = [n.lower() for n in ascii_names]
    df['person_name'] = ascii_names

    # Then geocode the addresses
    if geocode:
        print 'geocoding'
        addresses = df.person_address.dropna().drop_duplicates()
        clean_addresses = [re.sub('A14', 'U',  addr) for addr in addresses]

## First geocode all the addresses that we have
        geocoded_locales = []
        city_latlong_de = city_latlong[city_latlong.country == 'be']
        start_time = time.time()
        for idx, addr in enumerate(clean_addresses):
            if idx > 0 and idx % 1000 == 0:
                print idx
                print (time.time() - start_time) / idx
            gl = fuzzy_geocoder.fuzzy_city_check(addr.lower(), city_latlong_de, dmeta, 0.5)
            geocoded_locales.append(gl)
    
        locales = [g[0] for g in geocoded_locales]
        lats = [g[1] for g in geocoded_locales]
        lngs = [g[2] for g in geocoded_locales]
        addr_df = pd.DataFrame({'person_address': addresses,
                                'locale': locales,
                                'lat': lats,
                                'lng': lngs
                                }
                               )
        df = pd.merge(df, addr_df, on='person_address', how='left')

        # test for missing addresses
        df_name_addr = df[['person_name', 'person_address', 'lat']].drop_duplicates()
        nan_addr = np.array([nan_helper(l) for l in df_name_addr.lat])
        none_addr = np.equal(df_name_addr.lat, None)
        bool_without_addresses = np.logical_or(nan_addr, none_addr)

        # check names for address data
        names_without_addresses = df_name_addr.person_name[bool_without_addresses].dropna().drop_duplicates()
        names_without_addresses = [re.sub('A14', 'U', n) for n in names_without_addresses]
        names_without_addresses = [n for n in names_without_addresses if nap.filter_name(n)]
        geocoded_names = []
        name_fields = ['name', 'clean_name', 'locale', 'lat', 'lng']

        start_time = time.time()
        incr_time = start_time
        for idx, n in enumerate(names_without_addresses):
            if isinstance(n, str):
                if idx > 0 and idx % 1000 == 0:
                    print idx
                    print (time.time() - start_time) / idx
                    print (time.time() - incr_time) / 1000
                    incr_time = time.time()

            name, address = nap.parse_name(n)
        
            if address:
                gl = fuzzy_geocoder.fuzzy_city_check(address.lower(), city_latlong_de, dmeta, 0.5)
                name_locale = [n, name]
                name_locale.extend(gl)
                d_locale = dict(zip(name_fields, name_locale))
                geocoded_names.append(d_locale)

        name_addr_df = pd.DataFrame(geocoded_names)
        if name_addr_df.shape[0] != 0:
            df = pd.merge(df, name_addr_df, left_on='person_name', right_on='name', how='left')
            lat = [l[0] if nan_helper(l[1]) else l[1] for l in zip(df.lat_x, df.lat_y)]
            lng = [l[0] if nan_helper(l[1]) else l[1] for l in zip(df.lng_x, df.lng_y)]
            locale = [l[0] if isinstance(l[0], str) else l[1] for l in zip(df.locale_x, df.locale_y)]
        
            df = df[['appln_id', 'person_id', 'person_name', 'person_address',
                     'person_ctry_code', 'coauthors', 'ipc_code', 'year', 'clean_name']
                    ]
            df['lat'] = lat
            df['lng'] = lng
            df['locale'] = locale


    # Return the reformatted names and latlng pairs as two lists
    else:
        lats = [0.0] * df.shape[0]
        lngs = [0.0] * df.shape[0]
        df['lat'] = lats
        df['lng'] = lngs

    # Shorten the IPC codes to 4-digit
    ipc_codes = [md.sort_class(c, 4) for c in df['ipc_code']]
    df['ipc_code'] = ipc_codes
    # ascii and lowercase the coauthor data
    coauthors = md.asciidammit(df['coauthors'])
    coauthors = [c.lower() for c in coauthors]
    df['coauthors'] = coauthors

    df = df[['person_id', 'person_name', 'coauthors', 'ipc_code', 'lat', 'lng']]
    df.columns = ['Person', 'Name', 'Coauthor', 'Class', 'Lat', 'Lng']
    # Consolidate the records at the person_id level
    consolidate_dict = {'Name': cd.consolidate_unique,
                        'Lat': cd.consolidate_geo,
                        'Lng': cd.consolidate_geo,
                        'Class': cd.consolidate_set,
                        'Coauthor': cd.consolidate_set
                        }

    df_consolidated = cd.consolidate(df, 'Person', consolidate_dict)

    # Write out
    person_patent_out = '../data/dedupe_input/person_patent/' + country + '_person_patent_map.csv'
    f_out = '../data/dedupe_input/person_records/dedupe_input_' + country + '.csv'
    person_patent_map.to_csv(person_patent_out, index=False)
    df_consolidated.to_csv(f_out)
