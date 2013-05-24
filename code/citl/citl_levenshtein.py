import Levenshtein
from dedupe.distance import haversine
import pandas as pd
import re
import string
import fuzzy

def jaccard(s1, s2):
    numer = s1.intersection(s2)
    denom = s1.union(s2)
    return len(numer) / float(len(denom))

re_punct = re.compile('[%s]' % re.escape(string.punctuation))
def clean_name(n):
    n = re_punct.sub(' ', n)
    n = re.sub('\s{2,}', ' ', n)
    n = n.strip().lower()
    return n

dmeta_hash = fuzzy.DMetaphone(4)
def namehash(n, hashfun):
    return hashfun(n)[0]

citl_file_root = './%s/%s_citl_input.csv' 

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


n_matches = 3
all_record_matches = []
for country in eu27:
    try:
        input_file = citl_file_root % (country, country)
        input_df = pd.read_csv(input_file)
    except:
        continue
    print country
    input_df.name.fillna('')

    input_df['name'] = [clean_name(n) if isinstance(n, str) else None for n in input_df.name]
    input_df['namehash'] = [namehash(n, dmeta_hash) if isinstance(n, str) else None for n in input_df.name]
    input_df['name_split'] = [set(s.split(' ')) if isinstance(s, str) else None
                              for s in input_df.name
                              ]

    citl = input_df[input_df.source == 'citl']
    patstat = input_df[input_df.source == 'patstat']

    citl_iter = zip(citl.name, citl.id, citl.lat, citl.lng, citl.name_split, citl.namehash)
    patstat_iter = zip(patstat.name, patstat.id, patstat.lat, patstat.lng, patstat.name_split, patstat.namehash)

    import time
    start_time = time.time()
    country_record_matches = []
    for idx, citl_record in enumerate(citl_iter):
        if idx > 0 and (idx % 100) == 0:
            print idx
        record_dist = []
        for patstat_record in patstat_iter:
            if citl_record[-1] == patstat_record[-1]:#isinstance(patstat_record[0], str):
                lev_name_dist = Levenshtein.ratio(citl_record[0].lower(),
                                                  patstat_record[0].lower()
                                                  )
                jac_name_dist = jaccard(citl_record[4],
                                        patstat_record[4]
                                        )
                
            else:
                continue
            ## Convert from similarities to distances
            lev_name_dist = 1 - lev_name_dist
            jac_name_dist = 1 - jac_name_dist

            geo_dist = None
            if citl_record[1] != 0.0 and patstat_record[1] != 0.0:
                geo_dist = haversine.compareLatLong((citl_record[3],
                                                     citl_record[2]),
                                                    (patstat_record[3],
                                                     patstat_record[2])
                                                    )
                # Rescale to make the sort work: bigger values better,
                # like with Lev. ratio
                
                record_dist.append((citl_record[0],
                                    citl_record[1],
                                    patstat_record[0],
                                    patstat_record[1],
                                    lev_name_dist,
                                    jac_name_dist,
                                    geo_dist,
                                    country
                                    )
                                   )

        # Sort on name distance first, then geo distance
        sorted_dist = sorted(record_dist, key=lambda x: (x[4], x[5], x[6]))

        # Take the top N so we can filter for later
        citl_match = sorted_dist[:n_matches]
        
        country_record_matches.extend(citl_match)

    all_record_matches.extend(country_record_matches)
    end_time = time.time()
    country_time = (end_time - start_time) / 60
    print 'Finished %s in %f minutes' % (country, country_time)

df_out = pd.DataFrame(all_record_matches,
                      columns=['citl_name', 'citl_id',
                               'patstat_name', 'patstat_id',
                               'lev_name_dist', 'jac_name_dist',
                               'geo_dist', 'country']
                      )
df_out.to_csv('citl_patstat_matches.csv', index=False)
