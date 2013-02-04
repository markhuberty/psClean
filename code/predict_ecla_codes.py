from sklearn import cross_validation as cv
from sklearn import feature_extraction as fe
from sklearn import naive_bayes as nb
from sklearn.pipeline import FeatureUnion
import numpy as np
import os
import pandas as pd
import re
import sklearn

os.chdir('/home/markhuberty/Documents/psClean')

nl_data = pd.read_csv('./data/cleaned_data/cleaned_output_NL.tsv')
nl_data.set_index('appln_id')
nl_abstr = pd.read_csv('./data/nl_abstracts_raw.csv')
nl_abstr.set_index('appln_id')

nl_all = pd.merge(nl_data, nl_abstr, how='inner', left_on='appln_id', right_on='appln_id')

code_cats = {}
for t in test:
    if t[0] in code_cats:
        code_cats[t[0]] += 1
    else:
        code_cats[t[0]] = 1

labels = nl_abstr['ecla_class'].values

## define a generic for tfidf'ing strings
def generate_tfidf(str_list,
                   ngram_range=(1,1),
                   tfidf=True,
                   min_df=0.01,
                   max_df=0.99
                   str_sub=False,
                   delim=None
                   ):

    if str_sub:
        str_list = [re.sub(delim, ' ', s) for s in str_list]
    
    vectorizer = fe.text.CountVectorizer(ngram_range=ngram_range,
                                         #token_pattern=token_pattern,
                                         min_df=min_df,
                                         max_df=max_df
                                         )
    vectorizer_fit = vectorizer.fit(str_list)
    vectorizer_counts = vectorizer.transform(str_list)

    if tfidf:
        transformer = fe.text.TfidfTransformer(use_idf=True)
        transformer_fit = transformer.fit(vectorizer_counts)
        transformer_tfidf = transformer.transform(vectorizer_counts)
        return transformer_tfidf

    else:
        return vectorizer_counts


abstr_tfidf = generate_tfidf(nl_all['appln_abstract'])
ipc_tfidf = generate_tfidf(nl_data['ipc_codes'], str_sub=True, delim='**')

tfidf_union = FeatureUnion(abstr_tfidf, ipc_tfidf)

## Subset everything into training and testing

train_data, test_data, train_labels, test_labels = cv.train_test_split(
    tfidf_union, labels, test_size=0.1
    )
    )

## Generate a multinomial bayes classifier
mnb = nb.MultinomialNB()
ecla_classifier = mnb.fit(train_data, train_labels)
pred_train = ecla_classifier.predict(train_data)
pred_test = ecla_classifier.predict(test_data)

## Test predictive accuracy
def compute_accuracy(pred, actual):
    out = []
    for idx, est in pred:
        if est = actual[idx]:
            out.append(1)
        else:
            out.append(0)
    return np.mean(out)

train_accuracy = compute_accuracy(pred_train, train_labels)
test_accuracy = compute_accuracy(pred_test, test_labels)
