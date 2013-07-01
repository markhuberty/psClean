import pandas as pd
import sys
import os
import errno

#sys.path.append('/home/markhuberty/Documents/dedupe/examples/patent_example')
import patent_util

citl_file_root = '../../data/citl_data/geocoded/citl_geocoded_%s.csv'
patent_file_root = '../../data/dedupe_script_output/'

patent_files = os.listdir(patent_file_root)
patent_files = [f for f in patent_files if 'csv' in f]
countries = [f[-6:-4] for f in patent_files]
patent_file_dict = dict(zip(countries, patent_files))

for country in countries:
    print country

    citl_file = citl_file_root % country.upper()
    patent_file = patent_file_root + patent_file_dict[country]

    try:
        df_patent = pd.read_csv(patent_file)
    except:
        continue
    try:
        df_citl = pd.read_csv(citl_file)
    except:
        continue
    
    # Homogenize and write out
    df_citl = df_citl.drop_duplicates()

    df_patent = df_patent[['cluster_id', 'Name', 'Lat', 'Lng']]

    cluster_agg_dict = {'Name': patent_util.consolidate_unique,
                        'Lat': patent_util.consolidate_geo,
                        'Lng': patent_util.consolidate_geo,
                        }

    df_patent_consolidated = patent_util.consolidate(df_patent,
                                                     'cluster_id',
                                                     cluster_agg_dict
                                                     )


    df_patent_consolidated.columns = ['lat', 'lng', 'name']
    df_patent_consolidated['source'] = 'patstat'
    df_patent_consolidated['country'] = country
    df_patent_consolidated['id'] = df_patent_consolidated.index


    df_citl = df_citl[['accountholder', 'lat', 'lng', 'installationidentifier']]
    df_citl.columns = ['name', 'lat', 'lng', 'id']
    df_citl['country'] = country
    df_citl['source'] = 'citl'

    df_concat = pd.concat([df_patent_consolidated,
                           df_citl],
                          ignore_index=True
                          )

    try:
        os.mkdir('./%s' % country)
    except OSError as exception:
        if exception.errno != errno.EEXIST:
            raise
        
    output_file = './%s/%s_citl_input.csv' %(country, country)
    df_concat.to_csv(output_file,
                     index=False
                     )
