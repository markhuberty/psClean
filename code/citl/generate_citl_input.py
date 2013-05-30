import pandas as pd
import sys
sys.path.append('/home/markhuberty/Documents/dedupe/examples/patent_example')
import patent_util

citl_file_root = '../../data/citl_data/geocoded/citl_geocoded_%s.csv'
patent_file_root = '../dedupe/%s_weighted/patstat_output.csv'


eu27 = ['at',
        'bg',
        'be',
        'it',
        'gb',
        'fr',
        'de',
        'sk',
        'se',
        'pt',
        'pl',
        'hu',
        'ie',
        'ee',
        'es',
        'cy',
        'cz',
        'nl',
        'si',
        'ro',
        'dk',
        'lt',
        'lu',
        'lv',
        'mt',
        'fi',
        'el',
        ]

for country in eu27:
    citl_file = citl_file_root % country.upper()
    patent_file = patent_file_root % country

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

    df_patent = df_patent[['cluster_id_r1', 'Name', 'Lat', 'Lng', 'Class', 'patent_ct']]

    cluster_agg_dict = {'Name': patent_util.consolidate_unique,
                        'Lat': patent_util.consolidate_geo,
                        'Lng': patent_util.consolidate_geo,
                        'Class': patent_util.consolidate_set,
                        'patent_ct': np.sum
                        }

    df_patent_consolidated = patent_util.consolidate(df_patent,
                                                     'cluster_id_r1',
                                                     cluster_agg_dict
                                                     )

    ## Here, need to expand the returned data to include the Class
    df_patent_consolidated.columns = ['lat', 'lng', 'name', 'sector', 'patent_ct']
    df_patent_consolidated['source'] = 'patstat'
    df_patent_consolidated['country'] = country
    df_patent_consolidated['id'] = df_patent_consolidated.index

    ## Here, need to expand the returned data to include the mainactivitytypecodelookup
    df_citl = df_citl[['accountholder', 'lat', 'lng', 'installationidentifier', 'mainactivitytypecodelookup']]
    df_citl.columns = ['name', 'lat', 'lng', 'id', 'sector']
    df_citl['country'] = country
    df_citl['source'] = 'citl'
    df_citl['patent_ct'] = 'NA'

    df_concat = pd.concat([df_patent_consolidated,
                           df_citl],
                          ignore_index=True
                          )

    output_file = './%s/%s_citl_input.csv' %(country, country)
    df_concat.to_csv(output_file,
                     index=False
                     )
