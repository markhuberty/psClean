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
                name_dist = Levenshtein.ratio(citl_record[0].lower(),
                                              patstat_record[0].lower()
                                              )
                
            else:
                continue
            name_dist = 1 - name_dist

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
        sorted_dist = sorted(record_dist, key=lambda x: (x[2], x[3]))
        
        citl_match = list(sorted_dist[0])
        citl_match.append(country)
        
        country_record_matches.append(tuple(citl_match))

    all_record_matches.extend(country_record_matches)
    end_time = time.time()
    country_time = (end_time - start_time) / 60
    print 'Finished %s in %f minutes' % (country, country_time)

df_out = pd.DataFrame(all_record_matches,
                      columns=['citl_name', 'patstat_name', 'name_dist', 'geo_dist', 'country']
                      )
df_out.to_csv('citl_patstat_matches.csv', index=False)
# # Now given the data, train a classifier and predict.
# (citl_names, patstat_names, ldist, gdist) = zip(*[r for r in record_matches])

# def label_matches(r1, r2, d1, d2):
#     labels = []
#     d1_lab = []
#     d2_lab = []

#     counter = 0
#     idx_shuffle = np.random.shuffle
#     for name1, name2, dist1, dist2 in zip(r1, r2, d1, d2):

#         label = None
#         while label not in ['y', 'n', 'f']:
#             label = raw_input('Does %s match %s?' % (name1, name2))

#         if label=="f":
#             break

#         if label=="y":
#             labels.append(1)
#         else:
#             labels.append(0)
#         d1_lab.append(dist1)
#         d2_lab.append(dist2)
#         counter += 1
#         print '%i records labeled' % counter

#     return labels, d1_lab, d2_lab

# is_match, name_dist, geo_dist = label_matches(citl_names, patstat_names, ldist, gdist)

# from sklearn.naive_bayes import MultinomialNB
# import numpy as np
# from sklearn import cross_validation as cv

# mnb = MultinomialNB()

# name_dist_mean = np.mean(name_dist)
# geo_dist_mean = np.mean(geo_dist)

# name_dist_imp = [n if n else name_dist_mean for n in name_dist]
# geo_dist_imp = [g if g else geo_dist_mean for g in geo_dist]

# df_in = np.array([name_dist_imp, geo_dist_imp]).transpose()

# mnb_fit = mnb.fit(df_in, np.array(is_match))
# mnb_score = cv.cross_val_score(mnb, df_in, np.array(is_match),cv=10)
# p = mnb_fit.predict(df_in)

# from sklearn.svm import SVC
# svc = SVC()
# svc_fit = svc.fit(df_in, np.array(is_match))
# svc_pred = svc_fit.predict(df_in)
# svc_score = cv.cross_val_score(svc, df_in, np.array(is_match), cv=10)

# ldist_imp = [n if n else name_dist_mean for n in ldist]
# gdist_imp = [g if g and not np.isnan(g) else geo_dist_mean for g in gdist]

# df_pred = np.array([ldist_imp, gdist_imp]).transpose()

# lab_pred = svc_fit.predict(df_pred)

# matches = []
# for idx, l in enumerate(lab_pred):
#     if l == 1:
#         matches.append(record_matches[idx])
