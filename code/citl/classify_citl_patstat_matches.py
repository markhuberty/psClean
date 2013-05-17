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

dist_series = pd.Series(zip(df_match.lev_name_dist, df_match.jac_name_dist, df_match.geo_dist))
 
is_match, dists = label_matches(df_match.citl_name,
                                df_match.patstat_name,
                                dist_series
                                )


dist_mat = np.array(dists)

# Impute the missing values as means
lev_dist_mean = np.mean(dist_mat[:,0])
jac_dist_mean = np.mean(dist_mat[:,1])
geo_dist_mean = np.mean(dist_mat[:,2][~np.isnan(dist_mat[:,2])])

dist_mat[:, 0][np.isnan(dist_mat[:,0])] = lev_dist_mean
dist_mat[:, 1][np.isnan(dist_mat[:,1])] = jac_dist_mean
dist_mat[:, 2][np.isnan(dist_mat[:,2])] = geo_dist_mean

## mean-center everything
def scale(vec):
    vec_std = np.std(vec)
    vec_mean = np.mean(vec)
    vec_scale = (vec - vec_mean) / vec_std
    return vec_scale

dist_mat[:, 0] = scale(dist_mat[:, 0])
dist_mat[:, 1] = scale(dist_mat[:, 1])
dist_mat[:, 2] = scale(dist_mat[:, 2])


svc = SVC()

svc_fit = svc.fit(dist_mat, np.array(is_match))
svc_pred = svc_fit.predict(dist_mat)
svc_score = cv.cross_val_score(svc, dist_mat, np.array(is_match), cv=10)

pred_mat = df_match[['lev_name_dist', 'jac_name_dist', 'geo_dist']]
pred_mat.geo_dist.fillna(0, inplace=True)
out = svc_fit.predict(pred_mat)

df_match[out==1].to_csv('predicted_citl_matches.csv', index=False)
