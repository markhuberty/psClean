import pandas as pd
import networkx as nx
import numpy as np
import operator
import pdb

import sys
sys.path.append('/home/markhuberty/software/python-louvain')
import community


edgelist = pd.read_csv('../../data/nl_coauthor_edgelist.csv')
naics_labels = pd.read_csv('naics_ipc_df_new.csv')
naics_labels = naics_labels[naics_labels.country=='nl']
naics_labels.set_index(['cluster_id', 'naics'], inplace=True)
id_counts = naics_labels.groupby(level=0).size()
singular_ids = id_counts[id_counts == 1].index

naics_labels.reset_index(inplace=True)
naics_labels = naics_labels[naics_labels.cluster_id.isin(singular_ids)]
naics_labels.drop_duplicates(inplace=True)

nl_authors = pd.read_csv('../../data/patstat_consolidated_nl.csv')
nl_authors.set_index('cluster_id', inplace=True)

edgelist['name1'] = nl_authors.ix[edgelist.n1].Name.values
edgelist['name2'] = nl_authors.ix[edgelist.n2].Name.values

edgelist.name1.fillna('', inplace=True)
edgelist.name2.fillna('', inplace=True)

ebunch = []
nodes = {}
for idx, row in edgelist.iterrows():

    n1 = row['n1']
    n2 = row['n2']

    edge = tuple([n1, n2, row['weight']])
    ebunch.append(edge)
    if n1 not in nodes:
        nodes[n1] = row['name1']
    if n2 not in nodes:
        nodes[n2] = row['name2']

    
g = nx.Graph()
g.add_weighted_edges_from(ebunch)

## Add NAICS codes if you can
naics_labels.set_index('cluster_id', inplace=True)

counter = 0
for node in g.nodes():
    id = nodes[node]
    g.node[node]['name'] = id
    try:
        naics = str(int(naics_labels.ix[node]['naics']))
    except:
        naics = 'unknown'
    g.node[node]['naics'] = naics
    if naics != 'unknown':
        counter += 1

        
# Set up the label propagation

def propagate_labels(G, maxiter=10, generics=['5511', '5411', '5239', '5417']):
    naics_attr = nx.get_node_attributes(G, 'naics')
    unknown_counts = np.sum([1 if na=='unknown' else 0
                             for na in naics_attr.values()])
    unknown_diff = 1

    if unknown_counts == 0:
        print 'No unknown labels, exiting'
        return G
    else:
        print '%d unknown labels in G' % unknown_counts
    
    while_counter = 0
    while unknown_diff != 0 and while_counter < maxiter:
        print 'Change in unknowns: %d' % unknown_diff
        unknown_diff = 0
        for iter, n in enumerate(G.nodes_iter()):

            if iter % 10000 == 0:
                print 'Node %d, in while iteration %d' % (iter, while_counter)

            if g.node[n]['naics'] != 'unknown':
                continue
            n_neighbors = G[n]

            # pdb.set_trace()

            n_label_dict = {}
            for k in n_neighbors:
                l = G.node[k]['naics']
                if l != 'unknown' and not l in generics:
                    if l in n_label_dict:
                        n_label_dict[l] += 1
                    else:
                        n_label_dict[l] = 1

            # pdb.set_trace()
            if len(n_label_dict) > 0:
                new_label_idx = np.argmax(n_label_dict.values())
                new_label = n_label_dict.keys()[new_label_idx]
                G.node[n]['naics'] = new_label
                if new_label == 'unknown':
                    unknown_diff += 1
                else:
                    unknown_diff -= 1
        while_counter += 1
    return G

test_labels = propagate_labels(g, maxiter=100)

import re
def unroll_labels_to_names(G, df_names, re_firm):
    id_list = []
    naics_list = []
    for n in G:
        if not np.isnan(n):
            if 'naics' in G.node[n]:
                id_list.append(n)
                naics_list.append(G.node[n]['naics'])

    naics_df = pd.DataFrame({'naics': naics_list, 'id': id_list})
    out = pd.merge(df_names, naics_df, left_index=True, right_on='id')
    out.Name.fillna('', inplace=True)
    is_firm = [True if re_firm.search(n) else False for n in out.Name]
    out = out[is_firm]
    return out

test_unroll = unroll_labels_to_names(test_labels,
                                     nl_authors[['Name','patent_ct']],
                                     re.compile(' b v| n v')
                                     )

test_unroll.columns = ['company_name', 'patent_ct', 'patstat_id', 'naics']

test_unroll.to_csv('nl_naics_propagation_output_18July2013.csv',
                   index=False
                   )
