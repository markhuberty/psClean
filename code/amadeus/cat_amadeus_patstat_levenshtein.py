import Levenshtein
from dedupe.distance import haversine
import pandas as pd
import re
import re
import string
import fuzzy


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
dmeta_hash = fuzzy.DMetaphone(4)
def namehash(n, hashfun):
    return hashfun(n)[0]

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
                             'bvdep_id'
                             ]
                            ]

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

    
    # Hash the names for both amadeus and patstat
    df_firms['name_hash'] = [namehash(n, dmeta_hash) if isinstance(n, str) else None
                             for n in df_patstat.name]
    df_amadeus['name_hash'] = [namehash(n, dmeta_hash) if isinstance(n, str) else None
                               for n in df_amadeus.company_name]

    df_firms['name_split'] = [set(n.split(' ')) if instance(s, str) else None
                              for s in df_firms.name]
    df_amadeus['name_split'] = [set(n.split(' ')) if instance(s, str) else None
                                for s in df_amadeus.company_name]
    df_amadeus.set_index('name_hash', inplace=True)

    # Then walk over the firms and compute distances

    for idx, prow in df_firms.iterrows():
        df_amadeus_temp = df_amadeus.ix[row['name_hash']]

        if df_amadeus.shape[0] > 0:
            record_dist = []
            for idx, arow in df_amadeus_temp:
                lev_name_dist = Levenshtein.ratio(prow['name'].lower(),
                                                  arow['company_name'].lower()
                                                  )
                jac_name_dist = jaccard(prow['name_split'],
                                        arow['name_split']
                                        )
                lev_name_dist = 1 - lev_name_dist
                jac_name_dist = 1 - jac_name_dist

                geo_dist = None
                if citl_record[1] != 0.0 and patstat_record[1] != 0.0:
                    geo_dist = haversine.compareLatLong((prow['lat'],
                                                         prow['lng']),
                                                        (arow['latitude'],
                                                         arow['longitude'])
                                                        )
                record_dist.append(prow['name'],
                                   arow['company_name'],
                                   prow['dbase_id'],
                                   arow['bvdep_id'],
                                   lev_name_dist,
                                   jac_name_dist,
                                   geo_dist,
                                   country
                                   )
                
        else:
            continue




        sorted_dist = sorted(record_dist, key=lambda x: (x[4], x[5], x[6]))
        patstat_amadeus_match = sorted_dist[:n_matches]
        patstat_amadeus_matches.extend(patstat_amadeus_matches)

    all_record_matches.extend(patstat_amadeus_matches)

df_out = pd.DataFrame(all_record_matches,
                      columns=['patstat_name', 'amadeus_name',
                               'patstat_id', 'amadeus_id',
                               'lev_name_dist', 'jac_name_dist',
                               'geo_dist', 'country']
                      )
