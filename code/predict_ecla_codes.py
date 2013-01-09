from sklearn import cross_validation as cv
from sklearn import feature_extraction as fe
from sklearn import naive_bayes as nb
from sklearn.pipeline import FeatureUnion
import numpy as np
import os
import pandas as pd
import re
import sklearn

os.chdir()

nl_data = pd.read_csv()
nl_abstr = pd.read_csv()

labels = nl_abstr['ecla_class'].values

## define a generic for tfidf'ing strings
def generate_tfidf(str_list,
                   ngram_range=(1,1),
                   tfidf=True,
                   token_pattern=u'(?u)\b\w\w+\b',
                   str_sub=False,
                   delim=None
                   ):

    if str_sub:
        str_list = [re.sub(delim, ' ', s) for s in str_list]
    
    vectorizer = fe.text.CountVectorizer(ngram_range=ngram_range,
                                         token_pattern=token_pattern
                                         )
    vectorizer_fit = vectorizer.fit(str_list)
    vectorizer_counts = vectorizer.transform(str_list)

    if tfidf:
        transformer = fe.text.TfidfTransformer(use_idf=use_idf)
        transformer_fit = transformer.fit(vectorizer_counts)
        transformer_tfidf = transformer.transform(vectorizer_counts)
        return transformer_tfidf

    else:
        return vectorizer_counts


abstr_tfidf = generate_tfidf(nl_abstr['appln_abstr'].values)
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
