import fuzzy_geocoder
import fuzzy
import pandas as pd
import unidecode
import os
import numpy as np

source_dir = '../data/citl_data/countries/'
file_list = os.listdir(source_dir)
file_list = [f for f in file_list if 'csv' in f]

city_latlong = pd.read_csv('city_latlong.csv')
city_latlong.columns = ['country', 'city', 'lat', 'lng']

dmeta = fuzzy.DMetaphone(3)
city_latlong['city_hash'] = [dmeta(city)[0] for city in city_latlong.city]

for f in file_list:
    print f
    country = f[:2]
    f_in = source_dir + f
    df = pd.read_csv(f_in, encoding='utf-8')

    df['city'] = [unidecode.unidecode(c).lower() for c in df.city]
    city_list = city_latlong[city_latlong.country == country.lower()]
    geocoded_locales = []
    for city in df['city']:
        gl = fuzzy_geocoder.fuzzy_city_check(city, city_list, dmeta, 0.5)
        geocoded_locales.append(gl)

    locales = [g[0] for g in geocoded_locales]
    lats = [g[1] for g in geocoded_locales]
    lngs = [g[2] for g in geocoded_locales]
    addr_df = pd.DataFrame({'city': df['city'].values,
                            'locale': locales,
                            'lat': lats,
                            'lng': lngs
                            }
                           )

    df_out = pd.merge(df[['accountholder', 'name', 'city', 'installationidentifier', 'mainactivitytypecodelookup']],
                      addr_df[['city', 'lat', 'lng']],
                      left_on='city',
                      right_on='city',
                      how='left'
                      )
    df_out['accountholder'] = [unidecode.unidecode(unicode(acct)) for acct in df_out.accountholder]
    df_out['name'] = [unidecode.unidecode(unicode(n)) for n in df_out.name]
    df_out = df_out.drop_duplicates()
    output_file = 'citl_geocoded_' + country + '.csv'
    df_out.to_csv('../data/citl_data/geocoded/' + output_file,
                  index=False,
                  encoding='utf-8'
                  )
    ct_lat = len(df_out.lat.dropna())
    ct_ratio = float(ct_lat) / df_out.shape[0]
    print 'Pct addresses geocoded: %f' % ct_ratio

