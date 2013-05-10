import pandas as pd
from sklearn.svm import SVC
from sklearn import cross_validation as cv
import numpy as np


df_match = pd.read_csv('citl_patstat_matches.csv')

def label_matches(r1, r2, d1, d2):
    idx_shuffle = range(len(r1))
    np.random.shuffle(idx_shuffle)

    r1 = r1[idx_shuffle]
    r2 = r2[idx_shuffle]
    d1 = d1[idx_shuffle]
    d2 = d2[idx_shuffle]
    
    labels = []
    d1_lab = []
    d2_lab = []

    counter = 0
    for name1, name2, dist1, dist2 in zip(r1, r2, d1, d2):

        label = None
        while label not in ['y', 'n', 'f']:
            label = raw_input('Does %s match %s?' % (name1, name2))

        if label=="f":
            break

        if label=="y":
            labels.append(1)
        else:
            labels.append(0)
        d1_lab.append(dist1)
        d2_lab.append(dist2)
        counter += 1
        print '%i records labeled' % counter

    return labels, d1_lab, d2_lab

is_match, name_dist, geo_dist = label_matches(df_match.citl_name,
                                              df_match.patstat_name,
                                              df_match.name_dist,
                                              df_match.geo_dist 
                                              )

