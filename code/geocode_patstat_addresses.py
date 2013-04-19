import fuzzy_geocoder
import fuzzy
import pandas as pd
import os
import tfidf
import re
import time

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
df.to_csv('geocoded_de_data.csv', index=False)

# tfidf_test = tfidf.tfidf(df.person_name.values, 1)
# sorted_tfidf = sorted(tfidf_test.iteritems(), key=operator.itemgetter(1))
