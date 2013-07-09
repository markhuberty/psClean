import pandas as pd
import re
import sys
import os
import numpy as np

from sklearn import feature_extraction as fe

df = pd.read_csv('naics_ipc_df.csv')
df.set_index(['company_name', 'naics'], inplace=True)

df_agg = df.groupby(df.index).agg(agg_dict)

naics_ipc_df = pd.read_csv('naics_ipc_df_singles.csv')
df = naics_ipc_df.dropna()

df['ipc_1dig'] = [' '.join([i[0] for i in re.split('\s+', ipc.strip())])
                  if len(ipc) > 0 else '' for ipc in df.ipc_codes]


df['ipc_3dig'] = [' '.join([i[:3] for i in re.split('\s+', ipc.strip())])
                  if len(ipc) > 0 else '' for ipc in df.ipc_codes]

df['naics_2dig'] = [str(n)[:2] for n in df.naics]

naics_diverse = [str(n)[:2] if str(n)[0] not in ['2', '3', '4', '5'] else str(int(n))
                 for n in df.naics]

df['naics_diverse'] = naics_diverse

#df = df[~df.naics.isin([3339, 3329])]

#df = df[df.naics_2dig.isin(['31', '32', '33'])]

vectorizer = fe.text.CountVectorizer()#(token_pattern='\w{3}')
v_fit = vectorizer.fit(df.ipc_codes)
v_mat = vectorizer.transform(df.ipc_codes)

df_mat = pd.DataFrame(v_mat.todense(), index=df.naics_2dig)
df_blah = df_mat.groupby(level=0).agg(sum)
df_blah.to_csv('test_singles.csv')
maxcts = df_blah.apply(np.argmax, axis=1)

tf_transformer = fe.text.TfidfTransformer(use_idf=True).fit(v_mat)
v_mat_tfidf = tf_transformer.transform(v_mat)

import sklearn.cross_validation as cv
import sklearn.metrics as metrics
from sklearn.naive_bayes import MultinomialNB
nb = MultinomialNB()


nb_fit = nb.fit(v_mat_tfidf, df.naics.values)
nb_pred = nb_fit.predict(v_mat_tfidf)
nb_score = cv.cross_val_score(nb, v_mat_tfidf, df.naics.values, cv=5)

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

    
