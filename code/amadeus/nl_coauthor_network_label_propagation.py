import pandas as pd
import networkx as nx
import numpy as np
import operator
import pdb
import re


edgelist = pd.read_csv('../../data/nl_coauthor_edgelist.csv')
edgelist = edgelist.dropna()
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


re_firm = re.compile(' b v| n v')
# is_firm = [True if re_firm.search(n1) and re_firm.search(n2) else False
#            for n1, n2 in zip(edgelist.name1, edgelist.name2)]
# edgelist = edgelist[is_firm]

## Add NAICS codes if you can
naics_labels.set_index('cluster_id', inplace=True)

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

nl_authors.Name.fillna('', inplace=True)
node_size = nl_authors.ix[g.nodes()]['patent_ct']
node_name = nl_authors.ix[g.nodes()]['Name'] 
node_color = ['blue' if re_firm.search(n) else 'red' for n in node_name]

nx.set_node_attributes(g, 'size', dict(zip(g.nodes(), node_size)))
nx.set_node_attributes(g, 'name', dict(zip(g.nodes(), node_name)))
nx.set_node_attributes(g, 'color', dict(zip(g.nodes(), node_color)))

cc = nx.connected_component_subgraphs(g)
g_plot = cc[0]

g_plot_layout = nx.graphviz_layout(g_plot, 'sfdp')

edge_weight = nx.get_edge_attributes(g_plot, 'weight').values()
node_size = nx.get_node_attributes(g_plot, 'size').values()
node_size = [np.log10(n + 1) for n in node_size]

import matplotlib.pyplot as plt

nx.draw_networkx_edges(g_plot,
                       g_plot_layout,
                       edge_color=edge_weight,
                       edge_vmin=np.min(edge_weight),
                       edge_vmax=np.max(edge_weight),
                       edge_cmap=plt.cm.get_cmap('Blues'),
                       alpha=0.2,
                       width=0.2
                       )
nx.draw_networkx_nodes(g_plot,
                       g_plot_layout,
                       alpha=0.5,
                       node_size=node_size,
                       linewidths=0.05
                       )

plt.axis('off')
plt.savefig('nl_largest_cc_coauthor.png')
plt.close()



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

    if re_firm.search(g.node[node]['name']):
        g.node[node]['color'] = 'red'
    else:
        g.node[node]['color'] = 'blue'

    g.node[node]['size'] = nl_authors[nl_authors.Name==g.node[node]['name']].patent_ct

        
# Set up the label propagation

def propagate_labels(G, maxiter=10,
                     generics=['5511', '5411', '5239', '5417'],
                     weight=False,
                     label='label',
                     unk='unknown'):
    """
    Note that at present this returns a reference to the graph ,
    modifying the graph in place. That's a nuisance.
    """

    # G = G_input.copy()
    label_attr = nx.get_node_attributes(G, label)
    unknown_counts = np.sum([1 if l==unk else 0
                             for l in label_attr.values()])
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

            if G.node[n][label] != unk:
                continue
            n_neighbors = G[n]

            n_label_dict = {}
            for k in n_neighbors:
                l = G.node[k][label]
                if l != unk and not l in generics:
                    val = 1
                    if weight:
                        val = G[n][k]['weight']
                    if l in n_label_dict:
                        n_label_dict[l] += val
                    else:
                        n_label_dict[l] = val

            if len(n_label_dict) > 0:
                # pdb.set_trace()
                new_label_idx = np.argmax(n_label_dict.values())
                new_label = n_label_dict.keys()[new_label_idx]
                G.node[n][label] = new_label
                if new_label == unk:
                    unknown_diff += 1
                else:
                    unknown_diff -= 1
        while_counter += 1
    return G


test_labels = propagate_labels(g, maxiter=100, unk='unknown', label='naics')

def unroll_labels_to_names(G, df_names, re_firm, label):
    id_list = []
    naics_list = []
    for n in G:
        if not np.isnan(n):
            if 'naics' in G.node[n]:
                id_list.append(n)
                naics_list.append(G.node[n][label])

    naics_df = pd.DataFrame({'naics': naics_list, 'id': id_list})
    out = pd.merge(df_names, naics_df, left_index=True, right_on='id')
    out.Name.fillna('', inplace=True)
    is_firm = [True if re_firm.search(n) else False for n in out.Name]
    out = out[is_firm]
    return out

test_unroll = unroll_labels_to_names(test_labels,
                                     nl_authors[['Name','patent_ct']],
                                     re_firm,
                                     'naics'
                                     )

test_unroll.columns = ['company_name', 'patent_ct', 'patstat_id', 'naics']

test_unroll.to_csv('nl_naics_propagation_output_18July2013.csv',
                   index=False
                   )

# Randomly sample 100 rows for checking
import random
idx_random = sorted(test_unroll.index, key=lambda *args: random.random())
test_unroll_subset = test_unroll.ix[idx_random[:100]]

test_unroll_subset.to_csv('nl_naics_propagation_subset.csv')

## Test the validity of the match: randomly withhold N codes, then see whether we
## assign them correctly.

## Observed problem: we don't do really well at getting some codes right, largely b/c
## we're so sparse in the original labels. (And limited homophily)


import math
class boolRand:

    def __init__(self, p_true):
        if p_true > 0.5:
            self.p_true = 1 - p_true
        else:
            self.p_true = p_true

        n_len = int(math.ceil(1 / self.p_true))
        ct_true = int(math.floor(self.p_true * n_len))
        self.bool_vec = [True] * ct_true
        self.bool_vec.extend([False] * (n_len - ct_true))

    def __call__(self):
        return random.choice(self.bool_vec)


def cv_label_accuracy(edgelist, df_labels, n_folds=5, split=0.1):
    br = boolRand(split)

    # Build the graph
    ebunch = []
    for idx, row in edgelist.iterrows():

        n1 = row['n1']
        n2 = row['n2']

        edge = tuple([n1, n2, row['weight']])
        ebunch.append(edge)
    
    g_master = nx.Graph()
    g_master.add_weighted_edges_from(ebunch)

    #pdb.set_trace()
    # Then iter over the graph n_folds times, taking a difft split each time.
    cv_accuracy = []
    label_pairs = []
    for iter in range(n_folds):

        g = g_master.copy()
        reserved_labels = {}
        for node in g.nodes():
            id = nodes[node]
            g.node[node]['name'] = id
            try:
                naics = str(int(df_labels.ix[node]['naics']))
            except:
                naics = 'unknown'

            #pdb.set_trace()
            if naics != 'unknown':
                if br():
                    g.node[node]['naics'] = 'unknown'
                    reserved_labels[node] = naics
                else:
                    g.node[node]['naics'] = naics
            else:
                g.node[node]['naics'] = 'unknown'

        # pdb.set_trace()
        # Then propagate the labels
        g_out = propagate_labels(g, maxiter=100, unk='unknown', label='naics')

                # Then check the label accuracy
        bool_test = []
        bool_2dig_test = []
        node_match = []
        for node in g_out:
            if node in reserved_labels:
                node_naics = g_out.node[node]['naics']
                reserved_naics = reserved_labels[node]
                btest = node_naics == reserved_naics
                if node_naics != 'unknown':
                    btest_2dig = node_naics[:2] == reserved_naics[:2]
                    bool_2dig_test.append(btest_2dig)
                bool_test.append(btest)
                
                node_match.append((g_out.node[node]['naics'],
                                   reserved_labels[node]
                                   )
                                  )
                
        # pdb.set_trace()
        cv_accuracy.append((np.mean(bool_test), np.mean(bool_2dig_test)))
        print cv_accuracy                           
        label_pairs.append(node_match)

    return cv_accuracy, label_pairs

is_firm_edge = [True if re_firm.search(n1) and re_firm.search(n2) else False
                for n1, n2 in zip(edgelist.name1, edgelist.name2)]

firm_edgelist = edgelist[is_firm]
generics = [5511, 5411, 5239, 5417]
test_cv, cv_labels = cv_label_accuracy(firm_edgelist,
                                       naics_labels[~naics_labels.naics.isin(generics)],
                                       split=0.1
                                       )

edge_homophily_count = 0
total_edge_count = 0
for idx, edge in edgelist.iterrows():

    n1 = edge['n1']
    n2 = edge['n2']

    if n1 in naics_labels.index and n2 in naics_labels.index:
        l1 = naics_labels.ix[n1]['naics']
        l2 = naics_labels.ix[n2]['naics']
    else:
        continue
    total_edge_count += 1
    if l1 != 'unknown' and l2 != 'unknown':
        if str(l1)[:2] == str(l2)[:2]:
            edge_homophily_count += 1
        
cc = nx.connected_component_subgraphs(g)

pct_naics = []
for cg in cc:
    n_counter = 0
    for n in cg:
        if cg.node[n]['naics'] != 'unknown':
            n_counter += 1

    pct_naics.append(n_counter / float(len(cg)))
        
        
