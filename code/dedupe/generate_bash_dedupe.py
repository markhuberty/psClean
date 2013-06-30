import sys

"""
This script generates a bash script to disambiguate each country specified in the
country_weights dict. That dict should take the form country: precision_recall_weight. See the
dedupe documentation for those weights.

The dict provided herein documents the weights used for the disambiguation process for
data delivered in June 2013.

The script can be invoked from the command line :

python generate_bash_dedupe.py <path_to_root_psClean_directory>

It assumes the following:
- Dedupe inputs are in psClean/data/dedupe_input
- Dedupe output should go to psClean/data/dedupe_script_output

See patstat_dedupe.py for documentation of the dedupe process itself. 


"""

eu27_weights = {'at':3,
                'be':1.25,
                'bg':1.5,
                'cy': 1.5,
                'cz': 1.5,
                 'de':1.5,
                 'dk':1.5,
                 'ee':1.5,
                 'el':1.5,
                 'es':1.5,
                 'fi':1.5,
                 'fr':1.5,
                 'gb':2,
                'hu':5,
                'ie':3,
                'it':4.5,
                'lt':3,
                 'lu':1,
                 'lv':1.5,
                'mt':2.0,
                'nl':3.5,
                'pl':6,
                 'pt':1.5,
                 'ro':1.5,
                'se':1.0,
                 'si':1.5,
                 'sk':1.5
                }

ordered_weights = sorted(eu27_weights.items(), key=lambda v: v[0])

inputs = [i for idx, i in enumerate(sys.argv) if idx > 0]
rootdir = inputs[0]

base_str = 'python ' + \
           rootdir + '/code/dedupe/patstat_dedupe.py %s ' +  \
           rootdir + '/data/dedupe_input/person_records/ ' + \
           rootdir + '/data/dedupe_script_output %0.1f'


with open('eu27_dedupe_script.sh', 'a') as f:
    f.write('#!/bin/bash')
    f.write('\n')

for c, w in ordered_weights:
    output_str = base_str % (c, w)
    print output_str
    with open('eu27_dedupe_script.sh', 'a') as f:
        f.write(output_str)
        f.write('\n')

    
    
