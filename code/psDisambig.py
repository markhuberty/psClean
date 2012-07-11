############################
## Author: Mark Huberty, Mimi Tam, and Georg Zachmann
## Date Begun: 23 May 2012
## Purpose: Module to disambiguate inventor / assignee names in the PATSTAT patent
##          database
## License: BSD Simplified
## Copyright (c) 2012, Authors
## All rights reserved.
##
## Redistribution and use in source and binary forms, with or without
## modification, are permitted provided that the following conditions are met: 
## 
## 1. Redistributions of source code must retain the above copyright notice, this
##    list of conditions and the following disclaimer. 
## 2. Redistributions in binary form must reproduce the above copyright notice,
##    this list of conditions and the following disclaimer in the documentation
##    and/or other materials provided with the distribution. 
## 
## THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
## ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
## WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
## DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR
## ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
## (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
## LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
## ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
## (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
## SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
## 
## The views and conclusions contained in the software and documentation are those
## of the authors and should not be interpreted as representing official policies, 
## either expressed or implied, of the FreeBSD Project.
############################

import string
import scipy.sparse as sp
import numpy as np
import re
import itertools
import fuzzy

def build_ngram_dict(string_list, n=1):
    """
    Given a list of strings and a desired n-gram,
    builds an ngram dictionary of unique ngrams in the entire list
    Args:
      string_list: a list of strings to be parsed into ngrams
      param n:           an integer specifying the n-gram to use
    Returns:
      A list of unique ngrams in the string list
    """
    ngrams = []
    for s in string_list:
        for i in range((len(s) - n)):
            this_ngram = ''.join(s[j] for j in range(i, i + n))
            if this_ngram not in ngrams:
                ngrams.append(this_ngram)
    return ngrams


def build_ngram_freq(string_list, ngram_dict):
    """
    Given an ngram dictionary and a list of strings, returns the ngram frequency
    distribution for each ngram in each string

    Args:
      string_list:   a list of strings to be parsed into ngram frequencies
      ngram_dict:    a dictionary of unique ngrams to use in parsing the strings
    Returns:
      A list of dicts, one per string in string_list, with keys
      as ngrams and values as ngram frequencies in that string.
    """
    frequency_list = []
    for s in string_list:
        s_freq_dict = {}
        for n in ngram_dict:
            n_freq = len(re.findall(n, s))
            s_freq_dict[n] = n_freq
        frequency_list.append(s_freq_dict)
    return frequency_list


def build_ngram_mat(string_list, n=1):
    """
    Given a string list, build a sparse term-frequency matrix of ngram frequencies
    in each string

    Args:
      string_list:  a list of strings
      n:            an integer value indicating the ngram to use
    Returns:
      A scipy CSR matrix containing the ngram frequency vectors,
      with rows representing strings and columns representing ngrams
    """
    ngram_dictionary = build_ngram_dict(string_list, n)
    ngram_frequency = build_ngram_freq(string_list, ngram_dictionary)
    row_idx = []
    col_idx = []
    val = []
    for i, f in enumerate(ngram_frequency):
        row_idx.extend( [i] * len(f) )
        col_idx.extend( [ngram_dictionary.index(f_val) for f_val in f] )
        val.extend( [f[f_val] for f_val in f] )
    mat = sp.csc_matrix((np.array(val),
                         (np.array(row_idx),
                          np.array(col_idx)
                          )
                         )
                        )
    out = {'tf_matrix': mat,
           'ngram_dict': ngram_dictionary
           }
    return out


def build_incremental_ngram_mat(string_list, n=1):
    """
    Given a string list or a string iterator object, builds a sparse term-frequency
    matrix of ngram frequencies while only looking at each string once.

    Args:
      string_list:   a list of strings or an iterable string container
      n:             an integer indicating the ngram length to use
    Returns:
    A dict containing a scipy CSR matrix with the ngram frequency vectors, one row
    per string in string_list, one column per unique ngram in the string corpus;
    and the column ngram labels as a list
    """
    ## goal: do only one pass through the entire list so I can read incrementally
    ngram_dictionary = []
    ngram_frequency = []
    ngrams = []

    ## Generate the ngram frequency vectors one at a time
    ## and accumulate the unique ngrams into the dictionary
    for s in string_list:
        these_ngrams = set([''.join(s[j] for j in range(i, i + n))
                            for i in range((len(s) - n))]
                           )
        these_ngram_freqs = {}
        for ngram in these_ngrams:
            these_ngram_freqs[ngram] = len(re.findall(ngram, s))
            if ngram not in ngram_dictionary:
                ngram_dictionary.append(ngram)
        ngram_frequency.append(these_ngram_freqs)

    ## Then generate the tf matrix by using the dict to preserve column order
    row_idx = []
    col_idx = []
    val = []
    for i, f in enumerate(ngram_frequency):
        row_idx.extend([i] * len(f))
        col_idx.extend( [ngram_dictionary.index(f_val) for f_val in f] )
        val.extend( [f[f_val] for f_val in f] )

    if len(val) > 0:
        mat = sp.csc_matrix((np.array(val),
                             (np.array(row_idx),
                              np.array(col_idx)
                              )
                             )
                            )
        out = {'tf_matrix': mat,
               'ngram_dict': ngram_dictionary
               }
        
    else:
        out = {'tf_matrix': None,
               'ngram_dict': ngram_dictionary
               }
    return out


def build_leading_ngram_dict(name_list, leading_n=2):
    dict_out = {}
    for name in name_list:
        leading_letter_hash = name[0:(leading_n)]
        if leading_letter_hash in dict_out:
            dict_out[leading_letter_hash].append(name)
        else:
            dict_out[leading_letter_hash] = [name]
    return dict_out


def multiblock_ngram(name_list, block_1, leading_n=2):
    """
    Blocks names in two stages, first by the set of unique values in block_1,
    then by the leading_n set of letters in the name string
    Args:
       name_list: a list of name strings
       block_1:   a list of length name_list containing a set of unique
                  levels to block by
       leading_n: number of leading characters in names to block by after blocking by
                  block_1
    Returns:
       A nested dict of structure dict[block1 keys][leading_n keys]
    """
    dict_out = {}
    for block, name in zip(block_1, name_list):
        leading_letter_hash = name[0:leading_n]
        if block in dict_out:
            if leading_letter_hash in dict_out[block]:
                dict_out[block][leading_letter_hash].append(name)
            else:
                dict_out[block][leading_letter_hash] = [name]
        else:
            dict_out[block] = {leading_letter_hash:[name]}
    return dict_out


def cosine_similarity(mat):
        """
        Computes the cosine similarity with numpy linear algebra functions. For N
        strings with P features, returns an N*N sparse matrix of similarities. Should
        take and return scipy sparse matrix.
        """
        numerator = mat.dot(mat.transpose())
        denominator = sp.csc_matrix(np.sqrt(mat.multiply(mat).sum(axis=1)))
        denominator_out = denominator.dot(denominator.transpose())
        out = numerator / denominator_out
        return out


def cosine_similarity_match(mat, threshold=0.8):
    """
    Computes the cosine similarity between row X and whole matrix Y, then
    captures values with similarity >= threshold and returns the match index and
    similarity value.
    """
    print(mat.shape)
    print(mat.nnz) 
    nrows = mat.shape[0]
    matches_out = []
    for row in xrange(nrows):
        cosine_sim = rowwise_cosine_similarity(mat, mat[row, :])
        cosine_sim = cosine_sim.tocoo()
        matches = get_matches(cosine_sim, row, threshold)
        matches_out.append(matches)
    return(matches_out)


def get_matches(sim_mat, row_idx, threshold):
    """
    Filters a single row (sim_mat) in a sparse matrix for values greater than a threshold value.
    Args:
       sim_mat: a scipy sparse coo matrix of dimension 1 * N
       threshold: a threshold value (greater values = more restrictive)
    Returns:
       matches: a list of tuples of form (index, similiarity value)
    """
    matches = []
    for i,j,v in itertools.izip(sim_mat.row, sim_mat.col, sim_mat.data):
            if v >= threshold and j !=  row_idx:
                matches.append((j, v))
    return matches


def rowwise_cosine_similarity(mat, row):
    """
    Calculates the cosine similarity between one row of a matrix and all other rows.
    Returns a scipy sparse vector result.

    Args:
       mat: a scipy sparse matrix with rows as cases and columns as features
       row: an integer row number in mat
    Returns:
       A scipy sparse similarity matrix of dimension 1 * nrow(mat)
    """
    numerator = row.dot(mat.transpose())
    denominator_a = np.sqrt(row.multiply(row).sum(axis=1))
    denominator_b = np.sqrt(mat.multiply(mat).sum(axis=1))
    denominator = sp.csc_matrix(denominator_a[0,0] * denominator_b)
    cosine_sim = (numerator / denominator.transpose())
    return(cosine_sim)
