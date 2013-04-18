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

source_dir = '/mnt/db_master/patstat_raw/fleming_inputs/'
re_file = re.compile('cleaned_output_[A-Z]{2}.tsv$')
file_list = os.listdir(source_dir)
files = [f for f in file_list if re_file.match(f)]
file_header = ['', 'appln_id','person_id','person_name','person_address','person_ctry_code','firm_legal_id','coauthors','ipc_code','year']
dtypes = [np.int32, np.int32, np.int32, object, object, object, object, object, object, np.int32]
typedict = dict(zip(file_header, dtypes))

city_latlong = pd.read_csv('city_latlong.csv')

    
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
    try:
        geocode = True
        city_dict = global_city_dict[country]
    except KeyError:
        geocode = False
        

    # Clean up the names and ascii-ize them
    # First clean up names and find addresses
    ascii_names = md.asciidammit(df['person_name'])

    # Then lowercase everything
    ascii_names = [n.lower() for n in ascii_names]

    # Then geocode the addresses
    if geocode:
        
        cities, lats, lngs = zip(*[md.geocode_from_city_name(addr.lower(), city_dict) for addr in df['person_address']])  

        # Where are we missing addresses?
        missing_addr = [True if city == 'NONE' else False for city in cities]

        # For missing addresses, the address might be in the name. Find and geocode those names
        addr_candidates = pd.Series(ascii_names)[missing_addr].values

        # Then geocode from the names
        missing_names, missing_addrs, missing_lats, missing_lngs, missing_cities = md.find_address(addr_candidates,
                                                                                                   country,
                                                                                                   city_dict
                                                                                                   )

        ascii_names = pd.Series(ascii_names)
        cities = pd.Series(cities)
        lats = pd.Series(lats)
        lngs = pd.Series(lngs)
        
        ascii_names.ix[missing_addr] = missing_names
        cities.ix[missing_addr] = missing_cities
        lats.ix[missing_addr] = missing_lats
        lngs.ix[missing_addr] = missing_lngs

    # Return the reformatted names and latlng pairs as two lists
    else:
        lats = [0.0] * df.shape[0]
        lngs = [0.0] * df.shape[0]
    # Shorten the IPC codes to 4-digit
    ipc_codes = [md.sort_class(c, 4) for c in df['ipc_code']]

    # ascii and lowercase the coauthor data
    coauthors = md.asciidammit(df['coauthors'])
    coauthors = [c.lower() for c in coauthors]

    df_temp = pd.DataFrame({'Person': df.person_id,
                            'Name': ascii_names,
                            'Coauthor': coauthors,
                            'Class': ipc_codes,
                            'Lat': lats,
                            'Lng': lngs
                            }
                           )

    # Consolidate the records at the person_id level
    consolidate_dict = {'Name': cd.consolidate_unique,
                        'Lat': cd.consolidate_geo,
                        'Lng': cd.consolidate_geo,
                        'Class': cd.consolidate_set,
                        'Coauthor': cd.consolidate_set
                        }

    df_consolidated = cd.consolidate(df_temp, 'Person', consolidate_dict)

    # Write out
    person_patent_out = '../data/dedupe_input/person_patent/' + country + '_person_patent_map.csv'
    f_out = '../data/dedupe_input/person_records/dedupe_input_' + country + '.csv'
    person_patent_map.to_csv(person_patent_out, index=False)
    df_consolidated.to_csv(f_out)
