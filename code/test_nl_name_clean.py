import pandas as pd
import time
import modifications as md


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
nl_orig = pd.read_csv('/mnt/db_master/patstat_raw/fleming_inputs/cleaned_output_NL.tsv')
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
## Check for > 1 name in the name field

nl_data = md.do_all(nl_all)

addr_idx = pd.isnull(nl_data.Address)
names_without_addresses = nl_data.Name[addr_idx]

this_name, this_address, this_lat, this_lng, this_locality = md.find_address(names_without_addresses,
                                                                             'NL',
                                                                             city_latlng
                                                                             )
nl_data.Name[addr_idx] = [md.sort_name(n) for n in this_name]
nl_data.Address[addr_idx] = [md.clean_name(addr) for addr in this_address]
nl_data.Lat[addr_idx] = this_lat
nl_data.Lng[addr_idx] = this_lng
nl_data.Locality[addr_idx] = this_locality

nl_data.to_csv('/mnt/db_master/patstat_raw/disambig_input_data/nl_test_data_2Feb2013.csv')
