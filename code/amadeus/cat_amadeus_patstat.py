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

firm_ids = pd.read_csv('../../data/amadeus/company_legal_ids.csv')
abbrev_dict = {}
for idx, row in firm_ids.iterrows():
    country = row['country']
    country_abbrev = eu27[country]

    this_code = row['code'].lower()
    this_descr = row['descr'].lower()
    this_code = re.sub('\.', '', this_code)
    if country_abbrev in abbrev_dict:
        abbrev_dict[country_abbrev]['code'] += '|' + this_code + '|' + ' '.join(this_code)
        abbrev_dict[country_abbrev]['descr'] += '|' + this_descr
    else:
        abbrev_dict[country_abbrev] = {'code':this_code, 'descr': this_descr}

re_code_dict = {}
re_descr_dict = {}
for country in abbrev_dict:
    re_code_dict[country] = re.compile(abbrev_dict[country]['code'])
    re_descr_dict[country] = re.compile(abbrev_dict[country]['descr'])


amadeus_input_root = '../../data/amadeus/input/'
amadeus_output_root = '../../data/amadeus/patstat_amadeus/'
patstat_root = '../../data/dedupe_script_output/consolidated/'

for country in eu27:
    re_code = re_code_dict[country]
    re_descr = re_descr_dict[country]
    
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

    # Identify likely firms and dump the rest
    is_firm = [True if re_code.search(n) or re_descr.search(n) else False for n in df_patstat.Name]
    df_firms = df_patstat[is_firm]
    df_patstat.sort(columns=['patent_ct', 'coauthor_ct'],
                    ascending=[False, False],
                    inplace=True
                    )

    row_ct = int(0.1 * df_patstat.shape[0])
    df_patstat = pd.concat([df_firms, df_patstat[:row_ct]],
                           ignore_index=True
                           )
    
    
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


