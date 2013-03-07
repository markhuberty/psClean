import modifications as md
import numpy as np
import pandas as pd
import re
import time

city_df = pd.read_csv('/home/markhuberty/Documents/psClean/data/worldcitiespop.txt')
city_df = city_df[city_df.Country=='nl'][['City', 'Latitude', 'Longitude', 'Population']]
is_str = [True if isinstance(city, str) else False for city in city_df.City]
city_df = city_df[is_str]
city_upper = [city.upper() for city in city_df.City.values]
city_df.City = city_upper

## Check each city for duplicates, take the one w/ max pop
city_grouped = city_df.groupby('City')
city_count = city_grouped.size()
city_latlng = {}
for city in city_count.index:
    if city_count[city] > 1:
        pop_list = city_df.Population[city_df.City==city]
        max_pop = np.nanmax(pop_list)
        if not np.isnan(max_pop):
            lat = city_df.Latitude[(city_df.City==city) & (city_df.Population == max_pop)].values[0]
            lng = city_df.Longitude[(city_df.City==city) & (city_df.Population == max_pop)].values[0]
        else:
            lat = city_df.Latitude[city_df.City==city].values[0]
            lng = city_df.Longitude[city_df.City==city].values[0]
            
    else:
        lat = city_df.Latitude[city_df.City == city].values[0]
        lng = city_df.Longitude[city_df.City == city].values[0]
    city_latlng[city] = (lat, lng)

## Read in the original file
nl_orig = pd.read_csv('/mnt/db_master/patstat_raw/fleming_inputs/cleaned_output_NL.tsv',
                      sep="\t"
                      )
nl_fung = pd.read_csv('/mnt/db_master/patstat_raw/disambig_input_data/nl_test_data.csv')
nl_fung = nl_fung[['Patent', 'Person', 'Unique_Record_ID', 'Lat', 'Lng', 'Locality']]
## Merge them

nl_all = pd.merge(nl_fung,
                  nl_orig,
                  left_on=['Patent', 'Person'],
                  right_on=['appln_id', 'person_id'],
                  how='inner'
                  )
nl_all = nl_all[['appln_id', 'person_id', 'person_name',
                 'person_address', 'person_ctry_code', 'firm_legal_id',
                 'coauthors', 'ipc_code', 'year', 'Lat', 'Lng', 'Locality', 'Unique_Record_ID'
                 ]
                ]

nl_all.columns = ['Patent', 'Person', 'Name', 'Address', 'Country', 'LegalId', 'Coauthor',
                  'Class', 'Year', 'Lat', 'Lng', 'Locality', 'Unique_Record_ID'
                  ]
nl_all.Name.fillna('', inplace=True)
nl_all.Class.fillna('', inplace=True)
## Check for > 1 name in the name field

nl_data = md.do_all(nl_all)

addr_idx = pd.isnull(nl_data.Address)
names_without_addresses = nl_data.ix[addr_idx, "Name"].values

this_name, this_address, this_lat, this_lng, this_locality = md.find_address(names_without_addresses,
                                                                             'NL',
                                                                             city_latlng
                                                                             )
nl_data.ix[addr_idx, 'Name'] = this_name
nl_data['Name'] = [md.sort_name(n) for n in nl_data.Name.values]
nl_data.ix[addr_idx, 'Address'] = this_address
nl_data['Address'] = [md.clean_name(addr) for addr in nl_data.Address.values]
nl_data['Class'] = [md.sort_class(cl, 4) for cl in nl_data.Class.values]
nl_data.ix[addr_idx, "Lat"] = this_lat
nl_data.ix[addr_idx, "Lng"] = this_lng
nl_data.ix[addr_idx, "Locality"] = this_locality

## Make sure that everything is clean
def remove_commas(vec):
    out = [re.sub(',', ' ', str_in) if isinstance(str_in, str) else 'NONE'
           for str_in in vec]
    out = [str_in.strip() for str_in in out]
    return out

nl_data.Address = remove_commas(nl_data.Address.values)
nl_data.Coauthor = remove_commas(nl_data.Coauthor.values)
nl_data.Name = remove_commas(nl_data.Name.values)


nl_data.to_csv('/mnt/db_master/patstat_raw/disambig_input_data/nl_test_data_5March2013.csv',
               index=False
               )
