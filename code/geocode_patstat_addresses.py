from fuzzy_geocoder import *
import fuzzy
import pandas as pd
import os
import tfidf

input_dir = '/mnt/db_master/patstat_raw/fleming_inputs/'
input_file = input_dir + 'cleaned_output_DE.tsv'
sep='\t'

df = pd.read_csv(input_file, sep=sep)
city_latlong = pd.read_csv('../data/city_latlong.csv')
city_latlong.columns = ['country', 'city', 'lat', 'lng']

dmeta = fuzzy.DMetaphone(3)
city_latlong['city_hash'] = [dmeta(city)[0] for city in city_latlong.city]


tfidf_test = tfidf.tfidf(df.person_name.values)
sorted_tfidf = sorted(tfidf_test.iteritems(), key=operator.itemgetter(1))
