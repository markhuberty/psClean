import fuzzy_geocoder
import fuzzy
import pandas as pd
import os
import tfidf
import re
import time
import name_address_parser as nap

input_dir = '/mnt/db_master/patstat_raw/fleming_inputs/'
input_file = input_dir + 'cleaned_output_DE.tsv'
sep='\t'

df = pd.read_csv(input_file, sep=sep)

city_latlong = pd.read_csv('city_latlong.csv')
city_latlong.columns = ['country', 'city', 'lat', 'lng']

dmeta = fuzzy.DMetaphone(3)
city_latlong['city_hash'] = [dmeta(city)[0] for city in city_latlong.city]

addresses = df.person_address.dropna().drop_duplicates()
clean_addresses = [re.sub('A14', 'U',  addr) for addr in addresses]

## First geocode all the addresses that we have
geocoded_locales = []
city_latlong_de = city_latlong[city_latlong.country == 'de']
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

# Then locate addresses we don't have, and check names for
# embedded address information
idx_without_addresses = [True if l is None else False for l in df.lat]
names_without_addresses = df.person_name.ix[idx_without_addresses]
geocoded_names = []
name_fields = ['name', 'clean_name', 'locale', 'lat', 'lng']

for n in names_without_addresses:
    name, address = nap.parse_name(n)
    if address:
        gl = fuzzy_geocoder.fuzzy_city_check(address.lower(), city_latlong_de, dmeta, 0.5)
        name_locale = [n, name].extend(gl)
        d_locale = dict(zip(name_fields, name_locale))
        geocoded_names.append(d_locale)

name_addr_df = pd.DataFrame(geocoded_names)
df = pd.merge(df, name_addr_df, left_on='person_name', right_on='name', how='left')

df.to_csv('geocoded_de_data.csv', index=False)
