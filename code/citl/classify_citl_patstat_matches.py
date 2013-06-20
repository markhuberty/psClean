import pandas as pd
from sklearn.svm import SVC
from sklearn import cross_validation as cv
import numpy as np
import copy
from sklearn import feature_extraction as fe
import re

legal_identifiers = r"""\bbt\b|\bgmbh\b|\bpmdn\b|\boyj\b|\bepe\b|\brt\b|\bsgps\b|\bprc\b|\bohg\b|\bras\b|\bsas\b|\b\nspa\b|\bkb\b|\bgie\b|\btd\b|\bprp ltd\b|\bsnc\b|\bdba\b|\baps\b|\boe\b|\ba en p\b|\bext\b|\b\nkas\b|\bscs\b|\boy\b|\bsenc\b|\bapb\b|\bou\b|\bs de rl\b|\bgbr\b|\bkom srk\b|\bhb\b|\beeg\b|\b\nhf\b|\bldc\b|\bsk\b|\blda\b|\bpt\b|\bllp\b|\bsca\b|\bee\b|\bpty\b|\bllc\b|\bltda\b|\bscp\b|\b\npl\b|\bsoparfi\b|\beirl\b|\bgcv\b|\bjtd\b|\bev\b|\bca\b|\bsa\b|\bvof\b|\bsaica\b|\bkkt\b|\b\navv\b|\bsapa\b|\bsprl\b|\bspol sro\b|\bna\b|\binc\b|\bgesmbh\b|\bdoo\b|\bace\b|\bkol srk\b|\b\ns en c\b|\bkgaa\b|\bkdd\b|\bgmbh  co kg\b|\bkda\b|\baps  co ks\b|\basa\b|\bpma\b|\bnt\b|\b\ndd\b|\bnv\b|\btls\b|\bsp zoo\b|\bdno\b|\bsrl\b|\bcorp\b|\bltd\b|\belp\b|\beurl\b|\bcv\b|\b\npc ltd\b|\bkg\b|\bsarl\b|\bkd\b|\bkk\b|\bsp\b|\bbv\b|\bks\b|\bcvoa\b|\bplc\b|\bkv\b|\bsc\b|\b\nky\b|\bltee\b|\bbpk\b|\bibc\b|\bda\b|\bbvba\b|\bcva\b|\bkft\b|\bsafi\b|\beood\b|\bsa de cv\b|\b\ns en nc\b|\bamba\b|\bsdn bhd\b|\bac\b|\bab\b|\bae\b|\bad\b|\bag\b|\bis\b|\bans\b|\bal\b|\bas\b|\b\nood\b|\bvos\b|\bveb\b|\bco\b|\bb\sv\b|\bs\sa\b|\bs\sl\b|\bs\sp\sa\b|\bs\sl\b|\bsl|\bn\sv\b|\bspa\b|\bsro\b|\bs\sr\so\b|\bs\sr\sl\b|\ba\sg\b|\ba\sb\b|\bs\sa\ss\b|\bua\b"""



re_single_end = re.compile('\s\w$')
re_single_mid = re.compile('\s\w\s')
re_single_start = re.compile('^\w\s')

re_legal = re.compile(legal_identifiers)
df_match = pd.read_csv('citl_patstat_matches.csv')
re_space = re.compile(r'\s+')

re_single_ends = re.compile('^\w\s|\s\w$')

def strip_single(s):
    str_new = re_single_mid.sub(' ', s)

    str_delta = True
    while str_delta:
        str_old = str_new
        str_new = re_single_ends.sub('', str_old)
        str_delta = str_old != str_new
    return re_space.sub(' ', str_new).strip()

def strip_legal_or_single(s):
    out = strip_single(s)
    out = re_legal.sub(' ', out).strip()
    out = re_space.sub(' ', out)
    return out

def label_matches(r1, r2, dists, only_exact=True):
    row_idx = range(len(r1))
    labels = []
    dists_lab = []
    exact_match_ids = []
    match_ids = []

    exact_match_counter = 0
    for idx, name1, name2, dist in zip(row_idx, r1, r2, dists):
        name1_nolegal = strip_legal_or_single(name1)
        name2_nolegal = strip_legal_or_single(name2)
        name1_nospace_nolegal = re_space.sub('', name1_nolegal)
        name2_nospace_nolegal = re_space.sub('', name2_nolegal)
        name1_nospace = re_space.sub('', name1)
        name2_nospace = re_space.sub('', name2)
        
        if ((name1 == name2) or
            (name1_nospace_nolegal==name2_nospace_nolegal) or
            (name1_nospace == name2_nospace)):
            #labels.append(1)
            #dists_lab.append(dist)
            exact_match_ids.append(idx)
            exact_match_counter +=1


    print 'Found %i exact matches' % exact_match_counter

    match_ids.extend(exact_match_ids)
    # idx_shuffle = range(len(r1))
    # np.random.shuffle(idx_shuffle)

    
    # r1 = r1[idx_shuffle]
    # r2 = r2[idx_shuffle]
    # dists = dists[idx_shuffle]
    # row_idx = row_idx[idx_shuffle]

    label_counter = 0
    neg_counter = 0
    skipped_rounds = 0
    skip_counter = 0

    for idx, name1, name2, dist in zip(row_idx, r1, r2, dists):
        label = None

        name1_nolegal = strip_legal_or_single(name1)
        name2_nolegal = strip_legal_or_single(name2)
        name1_nospace_nolegal = re_space.sub('', name1_nolegal)
        name2_nospace_nolegal = re_space.sub('', name2_nolegal)
        name1_nospace = re_space.sub('', name1)
        name2_nospace = re_space.sub('', name2)
        
        if ((name1 == name2) or
            (name1_nospace_nolegal==name2_nospace_nolegal) or
            (name1_nospace == name2_nospace)):
            continue

        skip_test = (skip_counter > 0) and (skipped_rounds < skip_counter)
        if skip_test:
            skipped_rounds += 1
            continue
        
        while label not in ['y', 'n', 'f', 's']:
            label = raw_input('Does\n %s\n MATCH\n %s\n?' % (name1, name2))
            
        if label=="y" or label=='n':
            label_int = 1 if label=='y' else 0
            labels.append(label_int)
            label_counter += 1
            dists_lab.append(dist)
            match_ids.append(idx)
            if not only_exact and label == 'y':
                exact_match_ids.append(idx)
            if label=='n':
                neg_counter += 1
        elif label=='f':
            break
        elif label=='s':
            skip_counter = int(raw_input('How many to skip?\n'))
            skipped_rounds = 0

        print '%i records labeled, with %i neg' % (label_counter, neg_counter)

    return labels, dists_lab, exact_match_ids, match_ids


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
df_match['patent_ct'] = scale(df_match.patent_ct.values)


df_match.sort('lev_name_dist', ascending=True, inplace=True)

df_input = df_match[['citl_name', 'patstat_name', 'lev_name_dist', 'jac_name_dist',
                     'geo_dist']].drop_duplicates()

dist_series = pd.Series(zip(df_input.lev_name_dist, df_input.jac_name_dist, df_input.geo_dist))

is_match, dists, exact_matches, all_matches = label_matches(df_input.citl_name,
                                                            df_input.patstat_name,
                                                            dist_series,
                                                            only_exact=True
                                                            )

df_exact = df_input.iloc[exact_matches, :]
df_all = df_match.ix[df_input.index].iloc[all_matches, :]


## Insert sector classifier here.
## (1) return more stuff (i.e., ipc codes and CITL sectors or row IDs so we can get them) from label_matches
## (2) train a multiclass classifier to map IPCs:sectors. Would be useful to collapse "combustion" into a single category for that purpose
## (3) then take thec! distance metric is if(same_sector) TRUE else FALSE
## (4) then train the SVC w. the same sector metric.
## For (1), to do the doc-term stuff, just (a) load up the data frame and then (b)transform it into a list of strings, replacing the delimiter w. ** along the way


global_ipc_codes = [re.sub('\*\*', ' ', i) if isinstance(i, str) else ''
                    for i in df_match.ix[df_input.index].ipc_codes
                    ]
global_ipc_codes_2dig = [' '.join([c[:3] for c in g.split(' ')]) for g in global_ipc_codes]

vectorizer = fe.text.CountVectorizer()
v_fit = vectorizer.fit(global_ipc_codes_2dig)
v_mat = vectorizer.transform(global_ipc_codes_2dig)
v_mat_match = v_mat.todense()[all_matches, :]

from sklearn.feature_extraction.text import TfidfTransformer
tf_transformer = TfidfTransformer(use_idf=False).fit(v_mat)
v_mat_tfidf = tf_transformer.transform(v_mat)

v_mat_tfidf_match = v_mat_tfidf[all_matches, :]

combustion = 'Combustion installations with a rated thermal input exceeding 20 MW'
binary_sector = [1 if s == combustion else 0 for s in df_all.sector]

def balanced_sampler(s):
    vals = []
    idxs = []
    for idx, val in enumerate(s):
        if idx > 0:
            bal_score = np.sum(vals) / float(len(vals))
            if bal_score > 0.5:
                if val == 0:
                    vals.append(val)
                    idxs.append(idx)
            else:
                if val == 1:
                    vals.append(val)
                    idxs.append(idx)
        else:
            vals.append(val)
            idxs.append(idx)
    return vals, idxs

balanced_sector, balanced_index = balanced_sampler(binary_sector)
v_mat_bal = v_mat_match[balanced_index, :]

codes_svc = SVC()
kernal_grid = {'kernel':['rbf', 'poly', 'sigmoid']}
grid_svc = GridSearchCV(codes_svc,
                        kernal_grid,
                        score_func=metrics.zero_one_score,
                        cv=10
                        )
grid_svc_fit = grid_svc.fit(X=v_mat_match, y=np.array(binary_sector))

codes_fit = codes_svc.fit(v_mat_match,
                          np.array(binary_sector)
                          )
codes_cv_score = cv.cross_val_score(codes_svc,
                                    v_mat_tfidf_match,
                                    np.array(binary_sector),
                                    cv=10
                                    )

from sklearn.grid_search import GridSearchCV
import sklearn.cross_validation as cv
import sklearn.metrics as metrics
from sklearn.linear_model import Lasso, LassoCV, LogisticRegression
from sklearn.svm import l1_min_c
from sklearn.linear_model import lasso_path
from sklearn.linear_model import LassoCV, LogisticRegression
codes_lg = LogisticRegression(C=1.0, penalty='l1')
reg_values = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1]
grid_lasso = GridSearchCV(codes_lg,
                          [{'C':reg_values}],
                          score_func = metrics.zero_one_score,
                          cv=20
                          )
grid_lasso_fit = grid_lasso.fit(X=v_mat_match, y=np.array(binary_sector))
print grid_lasso_fit.best_params_, 1.0 - grid_lasso_fit.best_score_

codes_lg_best = LogisticRegression(C=grid_lasso_fit.best_params_['C'],
                                   penalty='l1'
                                   )
codes_lg_best_fit = codes_lg_best.fit(X=v_mat_bal,
                                      y=np.array(balanced_sector)
                                      )
print metrics.zero_one_score(np.array(balanced_sector),
                             codes_lg_best_fit.predict(v_mat_bal)
                             )

codes_lasso_fit = codes_lasso.fit(X=v_mat_bal, y=np.array(balanced_sector))



clf = GridSearchCV(LogisticRegression(C = 1.0, penalty = 'l1'),
                   c_grid,
                   score_func = metrics.zero_one_score,
                   cv = n_cv_folds
                   )
clf_fit = clf.fit(v_mat_match, np.array(binary_vector))

codes_lasso_cv = cv.cross_val_score(codes_lasso, v_mat_match, np.array(binary_sector))


dist_mat = np.array(dists)

svc = SVC()
svc_fit = svc.fit(dist_mat[:,:2], np.array(is_match))
svc_pred = svc_fit.predict(dist_mat[:,:2])
svc_score = cv.cross_val_score(svc, dist_mat[:,:2],
                               np.array(is_match),
                               cv=10
                               )
mean_svc_score = np.mean(svc_score)
sd_svc_score = np.std(svc_score)

from sklearn.linear_model import LogisticRegression
lg = LogisticRegression()
lg_fit = lg.fit(dist_mat[:,:2], np.array(is_match))
lg_pred = lg_fit.predict(dist_mat[:,:2])
lg_score = cv.cross_val_score(lg,
                              dist_mat[:,:2],
                              np.array(is_match),
                              cv=10)

from sklearn.naive_bayes import MultinomialNB
nb = MultinomialNB()
nb_fit = nb.fit(dist_mat, np.array(is_match))
nb_pred = nb_fit.predict(dist_mat)
nb_score = cv.cross_val_score(nb, dist_mat, np.array(is_match), cv=10)

pred_mat = df_match[['lev_name_dist', 'jac_name_dist', 'geo_dist']]
pred_mat = pred_mat[~pred_mat.index.isin(exact_matches)]

out = svc_fit.predict(pred_mat)

df_out = pd.concat([df_match[~df_match.index.isin(exact_matches)][out==1], df_exact], ignore_index=True)
df_out.to_csv("test_predicted_citl_matches.csv")

import operator
def count_codes_by_sector(codes, sectors):
    out = {}
    for c, s in zip(codes, sectors):
        if isinstance(c, str):
            c_split = c.split('**')
            if s not in out:
                out[s] = {}

            for cs in c_split:
                if cs in out[s]:
                    out[s][cs] += 1
                else:
                    out[s][cs] = 1
    
    for s in out:
        out[s] = sorted(out[s].iteritems(), key=operator.itemgetter(1))
    return out

test = count_codes_by_sector(df_all.ipc_codes, df_all.sector)
