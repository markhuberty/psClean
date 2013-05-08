import pandas as pd
sys.path.append('/home/markhuberty/Documents/dedupe/examples/patent_example')
import patent_util
citl_file = '../../data/citl_data/geocoded/citl_geocoded_NL.csv'
patent_file = '../code/dedupe/nl/patstat_output_2013-04-25_2.csv'

## Homogenize and write out
df_patent = pd.read_csv(patent_file)
df_citl = pd.read_csv(citl_file)

df_patent = df_patent[['cluster_id_r2', 'Name', 'Lat', 'Lng']]

cluster_agg_dict = {'Name': patent_util.consolidate_unique,
                    'Lat': patent_util.consolidate_geo,
                    'Lng': patent_util.consolidate_geo,
                    }

df_patent_consolidated = patent_util.consolidate(df_patent,
                                                 'cluster_id_r2',
                                                 cluster_agg_dict
                                                 )


df_patent_consolidated.columns = ['cluster_id', 'name', 'lat', 'lng']
df_patent_consolidated['source'] = 'patstat'


df_citl.columns = ['name', 'lat', 'lng']
df_citl['source'] = 'citl'

df_concat = pd.concat(df_patent_consolidated,
                      df_citl,
                      ignore_index=True
                      )
