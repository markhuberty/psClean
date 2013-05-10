import Levenshtein
from dedupe.distance import haversine
import pandas as pd

from IPython.parallel import Client

rc = Client()
dview = rc[:]

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

def jaccard(s1, s2):
    s1 = set(s1.split(' '))
    s2 = set(s2.split(' '))
    numer = s1.intersect(s2)
    denom = s1.union(s2)
    return len(numer) / float(len(denom))

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

    citl = input_df[input_df.source == 'citl']
    patstat = input_df[input_df.source == 'patstat']

    citl_iter = zip(citl.name, citl.lat, citl.lng)
    patstat_iter = zip(patstat.name, patstat.lat, patstat.lng)

    import time
    start_time = time.time()
    country_record_matches = []
    for idx, citl_record in enumerate(citl_iter):
        if idx > 0 and (idx % 100) == 0:
            print idx
        record_dist = []
        for patstat_record in patstat_iter:
            if isinstance(patstat_record[0], str):
                lev_name_dist = Levenshtein.ratio(citl_record[0].lower(),
                                                  patstat_record[0].lower()
                                                  )
                jac_name_dist = jaccard(citl_record[0].lower(),
                                        patstat_record[0].lower()
                                        )
                
            else:
                continue
            ## Convert from similarities to distances
            lev_name_dist = 1 - lev_name_dist
            jac_name_dist = 1 - jac_name_dist

            geo_dist = None
            if citl_record[1] != 0.0 and patstat_record[1] != 0.0:
                geo_dist = haversine.compareLatLong((citl_record[2],
                                                     citl_record[1]),
                                                    (patstat_record[2],
                                                     patstat_record[1])
                                                    )
                # Rescale to make the sort work: bigger values better,
                # like with Lev. ratio
                
                record_dist.append((citl_record[0],
                                    patstat_record[0],
                                    name_dist,
                                    geo_dist
                                    )
                                   )

        # Sort on name distance first, then geo distance
        sorted_dist = sorted(record_dist, key=lambda x: (x[2], x[3], x[4]))

        # Take the top N so we can filter for later
        citl_match = sorted_dist[:n_matches]
        citl_match.append(country)
        
        country_record_matches.extend(citl_match)

    all_record_matches.extend(country_record_matches)
    end_time = time.time()
    country_time = (end_time - start_time) / 60
    print 'Finished %s in %f minutes' % (country, country_time)

df_out = pd.DataFrame(all_record_matches,
                      columns=['citl_name', 'patstat_name', 'name_dist', 'geo_dist', 'country']
                      )
df_out.to_csv('citl_patstat_matches.csv', index=False)
