import sys
sys.path.append('/home/markhuberty/Documents/Research/Papers/psClean/code')
import psDisambig

## Demonstration code for ngram matrix generation
sample_strings = ['this is a string',
                  'my dog has fleas',
                  'we should really use actual data'
                  ]

test_unigram_mat = psDisambig.build_ngram_mat(sample_strings, n=1)
test_bigram_mat = psDisambig.build_ngram_mat(sample_strings, n=2)

print test_unigram_mat['tf_matrix'].todense()
print test_bigram_mat['tf_matrix'].todense()

test_incr_unigram_mat = psDisambig.build_incremental_ngram_mat(sample_strings, n=1)
