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

amadeus_input_root = '../../data/amadeus/input/'
amadeus_output_root = '../../data/amadeus/patstat_amadeus/'
patstat_root = '../../data/dedupe_script_output/consolidated/'

for country in eu27:
    print country
    amadeus_file = amadeus_input_root + 'clean_geocoded_%s.txt' % country.upper()
    patstat_file = patstat_root + 'patstat_consolidated_%s.csv' % country

    try:
        df_amadeus = pd.read_csv(amadeus_file, sep='\t')
    except:
        continue
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
    naics_str = ['' if np.isnan(n) else str(int(n)) for n in df_amadeus.ipc_sector]
    df_amadeus.ipc_sector = naics_str
    df_amadeus = df_amadeus.drop_duplicates()

    try:
        df_patstat = pd.read_csv(patstat_file)
    except:
        continue
    df_patstat = df_patstat[['Name',
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
    df_patstat['dbase'] = 'patstat'

    df_out = pd.concat([df_patstat, df_amadeus],
                       ignore_index=True,
                       axis=0
                       )

    outfile_name = amadeus_output_root + 'patstat_amadeus_input_%s.csv' % country
    df_out.to_csv(outfile_name)


