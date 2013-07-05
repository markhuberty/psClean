import pandas as pd


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

for country in eu27:

    amadeus_file = 'clean_geocoded_%s.txt' % country.upper()
    patstat_file = 'patstat_consolidated_%s.csv' % country

    df_amadeus = pd.read_csv(amadeus_file)
    df_amadeus = df_amadeus[['company_name',
                             'latitude',
                             'longitude',
                             'naics',
                             'id_bvdep'
                             ]
                            ]
    df_amadeus.columns = ['name',
                          'lat',
                          'lng',
                          'ipc_sector',
                          'dbase_id'
                          ]
    df_amadeus['dbase'] = 'amadeus'
    df_amadeus = df_amadeus.drop_duplicates()

    df_patstat = pd.read_csv(patstat_file)
    df_patstat = df_patstat[['Name'
                             'Lat',
                             'Lng',
                             'Class',
                             'cluster_id'
                             ]
                            ]
    df_patstat.columns = ['name',
                          'lat',
                          'lng',
                          'ipc_sector',
                          'dbase_id'
                          ]
    df_patstat = df_patstat.drop_duplicates()

    df_out = pd.concat([[df_patstat, df_amadeus]],
                       ignore_index=True
                       )

    outfile_name = 'patstat_amadeus_input_%s.csv' % country
    df_out.to_csv(outfile_name)
