import pandas as pd
from sklearn.svm import SVC
from sklearn import cross_validation as cv
import numpy as np
import copy
from sklearn import feature_extraction as fe

df_match = pd.read_csv('citl_patstat_matches.csv')

def label_matches(row_idx, r1, r2, dists):
    idx_shuffle = range(len(r1))
    np.random.shuffle(idx_shuffle)

    r1 = r1[idx_shuffle]
    r2 = r2[idx_shuffle]
    dists = dists[idx_shuffle]
    row_idx = row_idx[idx_shuffle]
    
    labels = []
    dists_lab = []
    exact_match_ids = []

    exact_match_counter = 0
    for idx, name1, name2, dist in zip(row_idx, r1, r2, dists):
        if name1 == name2:
            labels.append(1)
            dists_lab.append(dist)
            exact_match_ids.append(idx)
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

    return labels, dists_lab, exact_match_ids


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

is_match, dists, exact_matches = label_matches(df_match.index,
                                               df_match.citl_name,
                                               df_match.patstat_name,
                                               dist_series
                                               )

## Build the sector classifier
df_sector = df_match.ix[exact_matches][['sector', 'ipc_codes']]
print df_sector.sector.value_counts() / float(np.sum(df_sector.sector.value_counts()))

df_sector['ipc_codes'] = [re.sub('\*\*', ' ', ipc) if isinstance(ipc, str) else '' for ipc in df_sector.ipc_codes]
df_sector = df_sector.drop_duplicates()

ipc_codes_2dig = [' '.join(i[:2] for i in ipc.split(' ')) for ipc in df_sector.ipc_codes]

vectorizer = fe.text.CountVectorizer()
ipc_vectorizer = vectorizer.fit(ipc_codes_2dig)
codes_mat = ipc_vectorizer.transform(ipc_codes_2dig)
transformer = fe.text.TfidfTransformer()
ipc_transformer = transformer.fit(codes_mat)
tfidf_codes_mat = ipc_transformer.transform(codes_mat)

sector_crosswalk = {}
counter = 0
for s in df_match.sector.drop_duplicates():
    if s in sector_crosswalk:
        continue
    else:
        sector_crosswalk[s] = counter
        counter += 1

sector_labels = [sector_crosswalk[s] for s in df_sector.sector]

from sklearn.ensemble import RandomForestClassifier
rf = RandomForestClassifier(n_estimators=50)
rf_fit = rf.fit(X=codes_mat.toarray(), y=sector_labels)
rf_score = rf_fit.score(codes_mat.toarray(), sector_labels)
rf_cvscore = cv.cross_val_score(rf, codes_mat.toarray(), np.array(sector_labels), cv=5)
rf_pred = rf_fit.predict(codes_mat.toarray())

svc_sector = SVC()
svc_sector_fit = svc_sector.fit(codes_mat.toarray(), np.array(sector_labels))
svc_score = cv.cross_val_score(svc_sector, codes_mat.toarray(), np.array(sector_labels), cv=10)

from sklearn.naive_bayes import MultinomialNB
nb = MultinomialNB()
nb_sector_fit = nb.fit(codes_mat.toarray(), np.array(sector_labels))
nb_sector_score = cv.cross_val_score(nb, codes_mat.toarray(), np.array(sector_labels), cv=10)
nb_pred = nb_sector_fit.predict(codes_mat.toarray())

dist_mat = np.array(dists)

## Insert sector classifier here.
## (1) return more stuff (i.e., ipc codes and CITL sectors or row IDs so we can get them) from label_matches
## (2) train a multiclass classifier to map IPCs:sectors. Would be useful to collapse "combustion" into a single category for that purpose
## (3) then take thec! distance metric is if(same_sector) TRUE else FALSE
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
