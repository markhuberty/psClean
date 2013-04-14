import modifications as md
import numpy as np
import re
import time
import os
import pandas as pd
import AsciiDammit


file_list = os.listdir()
files = [f for f in file_list if 'cleaned_output_' in f]

city_file = '/home/markhuberty/Documents/psClean/data/worldcitiespop.txt'
city_list = pd.read_csv(city_file)
global_city_dict = md.build_city_dict(city_list)

for f in files:
    df = pd.read_csv(f, sep='\t')
    df.fillna('', inplace=True)
    country = df.person_ctry_code.values[0].lower()
    city_dict = global_city_dict[country]

    # Clean up the names and ascii-ize them
    # First clean up names and find addresses
    ascii_names = md.asciidammit(df['person_name'])

    # Then lowercase everything
    ascii_names = [n.lower() for n in ascii_names]

    # Then geocode the addresses
    cities, lats, lngs = [md.geocode_from_city_name(addr, city_dict) for addr in df['person_address']]

    # Where are we missing addresses?
    missing_addr = [True if city == 'NONE' for c in cities]

    # For missing addresses, the address might be in the name. Find and geocode those names
    addr_candidates = ascii_names[missing_addr]

    # Then geocode from the names
    missing_names, missing_addrs, missing_lats, missing_lngs, missing_cities = md.find_address(addr_candidates,
                                                                                               country,
                                                                                               city_dict
                                                                                               )

    ascii_names[missing_addr] = missing_names
    cities[missing_addr] = missing_cities
    lats[missing_addr] = missing_lats
    lngs[missing_addr] = missing_lngs

    # Return the reformatted names and latlng pairs as two lists
    
    # Shorten the IPC codes to 4-digit
    ipc_codes = [md.sort_class(c) for c in df['ipc_code']]

    # ascii and lowercase the coauthor data
    coauthors = md.asciidammit(df['coauthors'])
    coauthors = [c.lower() for c in coauthors]

    df_temp = pd.DataFrame({'Person': df.person_id
                            'Name': ascii_names,
                            'Coauthor': coauthors,
                            'Class': ipc_codes,
                            'Lat': lats,
                            'Lng': lngs
                            }
                           )
    
    # Consolidate the records at the person_id level
    consolidate_dict = {'Name': patent_util.consolidate_unique,
                        'Lat': patent_util.consolidate_geo,
                        'Lng': patent_util.consolidate_geo,
                        'Class': patent_util.consolidate_set,
                        'Coauthor': patent_util.consolidate_set
                        }

    df_consolidated = md.consolidate(df_temp, 'Person', consolidate_dict)

    # Write out
    f_out = 'dedupe_input_' + country + '.csv'
    df_consolidated.to_csv(f_out)
