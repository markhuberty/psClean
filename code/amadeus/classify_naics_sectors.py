import pandas as pd
import re
import sys
import os
import numpy as np

from sklearn import feature_extraction as fe

df = pd.read_csv('naics_ipc_df_new.csv')
df.set_index(['cluster_id', 'naics'], inplace=True)

# Reduce this to only 1:1 name:naics counts
naics_cluster_cts = df.groupby(level=0).size()
singleton_clusters = naics_cluster_cts[naics_cluster_cts==1].index
df.reset_index(inplace=True)
df_singular = df[df.cluster_id.isin(singleton_clusters)]

df_singular = df_singular.dropna()
df_singular = df_singular[df_singular.naics > 0]

df_singular['ipc_3dig'] = [' '.join([i[:3] for i in re.split('\s+', ipc.strip())])
                  if len(ipc) > 0 else '' for ipc in df_singular.ipc_codes]

df_singular['naics_2dig'] = [str(n)[:2] for n in df_singular.naics]

df_singular['naics_str'] = df_singular.naics.astype(str)
generics = ['5511', '5411', '5311', '5239']
df_singular = df_singular[~df_singular.naics_str.isin(generics)]

naics_cts = df_singular.naics_str.value_counts()
naics_tokeep = naics_cts[naics_cts > 10].index
df_singular = df_singular[df_singular.naics_str.isin(naics_tokeep)]
#df = df[~df.naics.isin([3339, 3329])]

#df = df[df.naics_2dig.isin(['31', '32', '33'])]

vectorizer = fe.text.CountVectorizer()#(token_pattern='\w{3}')
v_fit = vectorizer.fit(df_singular.ipc_codes)
v_mat = vectorizer.transform(df_singular.ipc_codes)

df_mat = pd.DataFrame(v_mat.todense(), index=df_singular.naics_2dig)
df_blah = df_mat.groupby(level=0).agg(sum)
df_blah.to_csv('test_singles.csv')
maxcts = df_blah.apply(np.argmax, axis=1)

tf_transformer = fe.text.TfidfTransformer(use_idf=True).fit(v_mat)
v_mat_tfidf = tf_transformer.transform(v_mat)

import sklearn.cross_validation as cv
import sklearn.metrics as metrics
from sklearn.naive_bayes import MultinomialNB
nb = MultinomialNB()


nb_fit = nb.fit(v_mat_tfidf, df_singular.naics_2dig.values)
nb_pred = nb_fit.predict(v_mat_tfidf)
nb_score = cv.cross_val_score(nb, v_mat_tfidf, df_singular.naics_2dig.values, cv=5)

nb_fit = nb.fit(v_mat, df.naics.values)
nb_pred = nb_fit.predict(v_mat)
nb_score = cv.cross_val_score(nb, v_mat, df.naics.values, cv=5)

from sklearn.ensemble import RandomForestClassifier 
rf = RandomForestClassifier()

class_integers = []
class_labels = {}
counter = 0
for n in df.naics_2dig:
    if n not in class_labels:
        class_labels[n] = counter
        counter += 1
    class_integers.append(class_labels[n])
    
rf_fit = rf.fit(v_mat_tfidf.todense(), np.array(class_integers))
rf_pred = rf_fit.predict(v_mat_tfidf.todense())
rf_score = cv.cross_val_score(rf, v_mat_tfidf.todense(), np.array(class_integers), cv=5)

nb_fit = nb.fit(v_mat, np.array(df.naics_2dig))
nb_pred = nb_fit.predict(v_mat)
nb_score = cv.cross_val_score(nb, v_mat, np.array(df.naics_2dig), cv=10)


from sklearn import svm
SVC = svm.SVC()

svc_fit = SVC.fit(v_mat, df.naics_2dig.values)
svc_pred = svc_fit.predict(v_mat)
svc_score = cv.cross_val_score(SVC, v_mat, df.naics_2dig.values, cv=5)


Pred_mat = df_match[['lev_name_dist', 'jac_name_dist', 'geo_dist']]
pred_mat = pred_mat[~pred_mat.index.isin(exact_matches)]

    
