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
    amadeus_file = amadeus_input_root + 'clean_geocoded_%s.txt' % country.upper()

    df = pd.read_csv(amadeus_file, sep='\t')
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
                  'naics_2007'm
                  'lat',
                  'long',
                  'is_bvdep_id',
                  'd_uo_bvdep_id',
                  'g_up_bvdep_id'
                  ]
    df['country'] = country

    # Dump prior rows for same country
    cursor = db.cursor()
    cursor.execute(delete_query % country)

    # Write in the new data
    psql.write_frame(df, 'amadeus_parent_child', if_exists='append', con=db, flavor='mysql')

    

    
