import pandas as pd
import MySQLdb
import pandas.io.sql as psql

amadeus_input_root = '../../data/amadeus/input/'

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
        'gr'
        ]

db=MySQLdb.connect(host='localhost',
                   port = 3306,
                   user='markhuberty',
                   passwd='patstat_huberty',
                   db = 'patstatOct2011'
                   )

delete_query = """DELETE FROM amadeus_parent_child WHERE country='%s'"""

for country in eu27:
    print country
    amadeus_file = amadeus_input_root + 'large_%s.txt' % country.upper()

    try:
        df_chunker = pd.read_csv(amadeus_file, sep='\t', chunksize=100000)
    except:
        continue

    
    # Dump prior rows for same country
    cursor = db.cursor()
    cursor.execute(delete_query % country)

    for df in df_chunker:
        df = df[['bvdep id number',
                 'company name',
                 'naics 2007 core code',
                 'Latitude',
                 'Longitude',
                 'immediate shareholder bvdep id number',
                 'domestic uo bvdep id number',
                 'global uo bvdep id number'
                 ]
                ]

        df.columns = ['bvdep_id',
                      'company_name',
                      'naics_2007',
                      'lat',
                      'lng',
                      'is_bvdep_id',
                      'd_uo_bvdep_id',
                      'g_uo_bvdep_id'
                      ]

        df['country'] = country

        df['bvdep_id'] = df['bvdep_id'].astype(str)
        df['is_bvdep_id'] = df['is_bvdep_id'].astype(str)
        df['d_uo_bvdep_id'] = df['d_uo_bvdep_id'].astype(str)
        df['g_uo_bvdep_id'] = df['g_uo_bvdep_id'].astype(str)

    
        df['company_name'].fillna('', inplace=True)
        df['lat'].fillna(0.0, inplace=True)
        df['lng'].fillna(0.0, inplace=True)
        df['naics_2007'].fillna(0, inplace=True)
        df['is_bvdep_id'].fillna('NONE', inplace=True)
        df['d_uo_bvdep_id'].fillna('NONE', inplace=True)
        df['g_uo_bvdep_id'].fillna('NONE', inplace=True)
        
        # Write in the new data
        psql.write_frame(df, 'amadeus_parent_child', if_exists='append', con=db, flavor='mysql')

    

    
