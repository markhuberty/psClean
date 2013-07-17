import pandas as pd
import re

eu27_abbrev = {'austria':'at',
               'bulgaria':'bg',
               'belgium':'be',
               'italy':'it',
               'uk':'gb',
               'france':'fr',
               'germany':'de',
               'slovakia':'sk',
               'sweden':'se',
               'portugal':'pt',
               'poland':'pl',
               'hungary':'hu',
               'ireland':'ie',
               'estonia':'ee',
               'spain':'es',
               'cyprus':'cy',
               'czech republic':'cz',
               'netherlands':'nl',
               'slovenia':'si',
               'romania':'ro',
               'denmark':'dk',
               'lithuania':'lt',
               'luxembourg':'lu',
               'latvia':'lv',
               'malta':'mt',
               'finland':'fi',
               'greece':'el',
               'greece':'gr'
               }


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

firm_ids = pd.read_csv('company_legal_ids.csv')
abbrev_dict = {}
for idx, row in firm_ids.iterrows():
    country = row['country'].lower()

    try:
        country_abbrev = eu27_abbrev[country]
    except KeyError:
        continue

    this_code = re.sub(' ', '', row['codes_ascii'].lower())
    this_code = '^' + this_code + '|' + this_code + '$'
    this_descr = row['description_ascii'].lower().split(' ')[0]
    this_descr = '^' + this_descr + '|' + this_descr + '$'
    regex_code = re.sub('\.', '', this_code)

    # regex_code = this_code + '|' + re.sub('\s{2,}', ' ', ' '.join(this_code))
    # regex_code = re.sub('\^ ', '^', regex_code)
    # regex_code = re.sub(' \$', '$', regex_code)

    if country_abbrev in abbrev_dict:
        abbrev_dict[country_abbrev]['code'] += '|' + regex_code
        abbrev_dict[country_abbrev]['descr'] += '|' + this_descr
    else:
        abbrev_dict[country_abbrev] = {'code':regex_code, 'descr': this_descr}

re_code_dict = {}
re_descr_dict = {}
for country in abbrev_dict:
    re_code_dict[country] = re.compile(abbrev_dict[country]['code'])
    re_descr_dict[country] = re.compile(abbrev_dict[country]['descr'])


amadeus_input_root = '../../data/amadeus/input/'
amadeus_output_root = '../../data/amadeus/patstat_amadeus/'
patstat_root = '../../data/dedupe_script_output/consolidated/'

for country in eu27:
    if country in re_code_dict:
        re_code = re_code_dict[country]
        re_descr = re_descr_dict[country]
    else:
        re_code, re_descr = None, None
    
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
    df_patstat.Name.fillna('', inplace=True)
    coauthor_ct = [len(c.split('**')) if isinstance(c, str) else 0
                   for c in df_patstat.Coauthor]
    df_patstat['coauthor_ct'] = coauthor_ct
    df_patstat.sort(columns=['patent_ct', 'coauthor_ct'],
                    ascending=[False, False],
                    inplace=True
                    )

    row_ct = int(0.1 * df_patstat.shape[0])
    df_merge = df_patstat[:row_ct]
    
    if re_code:
        is_firm = [True if re_code.search(re.sub('\s+', '', n))
                   or re_descr.search(re.sub('\s+', '', n)) else False
                   for n in df_patstat.Name]
        df_firms = df_patstat[is_firm]

    
    df_merge = pd.concat([df_merge, df_firms], ignore_index=True)
    
    df_patstat = df_merge[['Name',
                           'Lat',
                           'Lng',
                           'Class',
                           'cluster_id',
                           'patent_ct',
                           'coauthor_ct'
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


