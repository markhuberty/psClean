import pandas as pd
import numpy as np
import scipy.stats as ss


def compute_bayes_probs(df_ipc):
    """
    Inputs:
    df_ipc: a matrix of dimension R * I for R records and I ipc codes, indexed by S_r (the sector corresponding to record R)
    df_ipc should be a Pandas dataframe.

    Process: computes P(I | S), then inverts the probability for P(S | I). Returns a matrix of probabilities where each
    cell is equivalent to P(I_i | S_s).

    Outputs:
    sector_ipc_probs: a matrix of dimension S * I for S sectors and I ipc codes, indexed by sector
    sector_ipc_probs is a Pandas dataframe.
    """

    ipc_counts = df_ipc.sum(axis=0)
    sector_ipc_counts = df_ipc.groupby(level=0).sum()

    ipc_probs = ipc_counts / ipc_counts.sum()
    sector_probs = sector_ipc_counts.sum(axis=1) / sector_ipc_counts.sum().sum()

    ipc_sector_probs = sector_ipc_counts.divide(sector_ipc_counts.sum(axis=1), axis='index')
    ipc_sector_probs.fillna(0, inplace=True)

    sector_ipc_probs = ipc_sector_probs.divide(ipc_probs, axis='columns').mul(sector_probs, axis='index')
    sector_ipc_probs.fillna(0, inplace=True)

    return sector_ipc_probs, ipc_probs
    

def compute_si_probs(df_ipc):

    """
    Inputs:
    df_ipc: a matrix of dimension R * I for R records and I ipc codes, indexed by S_r (the sector corresponding to record R)
    df_ipc should be a Pandas dataframe.

    Process: divides each cell in df_ipc by the column sum for that column, and the row sum for that row.

    Outputs:
    sector_si_probs: a matrix of dimension S * I for S sectors and I ipc codes, indexed by sector.
    sector_si_probs is a Pandas dataframe
    """
    
    sector_ipc_counts = df_ipc.groupby(level=0).sum()
    ipc_probs = pd.Series([1] * df_ipc.shape[1], index=df_ipc.columns)

    row_sums = sector_ipc_counts.sum(axis=1)
    col_sums = sector_ipc_counts.sum(axis=0)
    
    sector_si_probs = sector_ipc_counts.divide(row_sums, axis='index').divide(col_sums, axis='columns')
    sector_si_probs.fillna(0, inplace=True)

    return sector_si_probs, ipc_probs


def build_ipc_matrix(ipcs, sectors):
    """
    ipcs should be a list of lists, each list contain IPC codes, as in:
    [['e05', 'a61'], ['a01', 'b65'], ...]
    """
    ipc_counts = []
    for ipc_list in ipcs:
        ipc_dict = {}
        for ipc in ipc_list:
            if ipc != ' ' and ipc != '':
                if ipc in ipc_dict:
                    ipc_dict[ipc] += 1
                else:
                    ipc_dict[ipc] = 1
        ipc_counts.append(ipc_dict)
    df_counts = pd.DataFrame(ipc_counts, index=sectors)
    return df_counts


class ipcSectorCompare:

    def __init__(self, sectors, raw_ipc_list, prob_fun=compute_si_probs):
        self.sectors = sectors
        self.ipc_mat = build_ipc_matrix(raw_ipc_list, self.sectors)
        self.prob_matrix, self.ipc_probs = prob_fun(self.ipc_mat)

        self.weighted_probs = self.prob_matrix * self.ipc_probs
        self.prob_dict = self.weighted_probs.to_dict()

    def __call__(self, sector, ipcs, distance_type='prob'):

        if distance_type not in ['rank', 'prob']:
            raise ValueError("distance_type must be one of rank or prob")

        sector_probs = self.query_all_sector_probs(ipcs)

        if sector_probs is not None:
            if distance_type == 'prob':
                dist_val = sector_probs.ix[sector]
            else:
                sector_ranks = sector_probs.rank(ascending=False)
                dist_val = sector_ranks.ix[sector]

            return dist_val
        return np.nan


    def query_all_sector_probs(self, ipcs):
        """
        Given some IPCS, return the sector probabilities for all sectors
        """

        # We have to trap potential indexerrors:
        #ipcs = [i for i in ipcs if i in self.prob_matrix.columns]

        if len(ipcs) > 0:
            #weighted_s_probs = self.prob_matrix * self.ipc_probs
            subset_s_probs = self.weighted_probs[ipcs]
            p_s = subset_s_probs.sum(axis=1)
            return p_s
        return None
    
