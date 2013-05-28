import pandas as pd
from sklearn.svm import SVC
from sklearn import cross_validation as cv
import numpy as np


df_match = pd.read_csv('citl_patstat_matches.csv')

def label_matches(r1, r2, dists):
    idx_shuffle = range(len(r1))
    np.random.shuffle(idx_shuffle)

    r1 = r1[idx_shuffle]
    r2 = r2[idx_shuffle]
    dists = dists[idx_shuffle]
    
    labels = []
    dists_lab = []

    exact_match_counter = 0
    for name1, name2, dist in zip(r1, r2, dists):
        if name1 == name2:
            labels.append(1)
            dists_lab.append(dist)
            exact_match_counter +=1

    print 'Found %i exact matches' % exact_match_counter

    label_counter = 0
    neg_counter = 0
    for name1, name2, dist in zip(r1, r2, dists):
        
        label = None
        if name1 == name2:
            continue

        while label not in ['y', 'n', 'f']:
            label = raw_input('Does\n %s\n MATCH\n %s\n?' % (name1, name2))
            
            if label=="f":
                break

            if label=="y":
                labels.append(1)
            else:
                labels.append(0)
                neg_counter += 1
            dists_lab.append(dist)
            label_counter += 1

        if label=="f":
            break
        print '%i records labeled, with %i neg' % (label_counter, neg_counter)

    return labels, dists_lab


# Rescale df_match first

# Set the geo dist values that are NA to the mean

df_match.geo_dist.fillna(df_match.geo_dist.mean(), inplace=True)

## mean-center everything
def scale(vec):
    vec_std = np.std(vec)
    vec_mean = np.mean(vec)
    vec_scale = (vec - vec_mean) / vec_std
    return vec_scale

df_match['lev_name_dist'] = scale(df_match.lev_name_dist.values)
df_match['jac_name_dist'] = scale(df_match.jac_name_dist.values)
df_match['geo_dist'] = scale(df_match.geo_dist.values)

dist_series = pd.Series(zip(df_match.lev_name_dist, df_match.jac_name_dist, df_match.geo_dist))

is_match, dists = label_matches(df_match.citl_name,
                                df_match.patstat_name,
                                dist_series
                                )

dist_mat = np.array(dists)

## Insert sector classifier here.
## (1) return more stuff (i.e., ipc codes and CITL sectors or row IDs so we can get them) from label_matches
## (2) train a multiclass classifier to map IPCs:sectors. Would be useful to collapse "combustion" into a single category for that purpose
## (3) then take the distance metric is if(same_sector) TRUE else FALSE
## (4) then train the SVC w. the same sector metric.
## For (1), to do the doc-term stuff, just (a) load up the data frame and then (b)transform it into a list of strings, replacing the delimiter w. ** along the way

svc = SVC()
svc_fit = svc.fit(dist_mat, np.array(is_match))
svc_pred = svc_fit.predict(dist_mat)
svc_score = cv.cross_val_score(svc, dist_mat, np.array(is_match), cv=10)
mean_svc_score = np.mean(svc_score)
sd_svc_score = np.std(svc_score)

pred_mat = df_match[['lev_name_dist', 'jac_name_dist', 'geo_dist']]
pred_mat.geo_dist.fillna(0, inplace=True)
out = svc_fit.predict(pred_mat)

df_out = df_match[out==1]
df_match[out==1].to_csv('predicted_citl_matches.csv', index=False)
