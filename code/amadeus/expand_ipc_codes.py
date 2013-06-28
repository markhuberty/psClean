import sys
import os
import pandas as pd

inputs = [i for idx, i in enumerate(sys.argv) if idx > 0]
raw_input_dir = inputs[0]
dedupe_input_dir = inputs[1]
output_dir = inputs[2]

eu27 = ['at',
        'bg',
        'be',
        'it',
        'gb',
        'fr',
        'de',
        'sk',
        'se',
        'pt',
        'pl',
        'hu',
        'ie',
        'ee',
        'es',
        'cy',
        'cz',
        'nl',
        'si',
        'ro',
        'dk',
        'lt',
        'lu',
        'lv',
        'mt',
        'fi',
        'el',
        'gr'
        ]

output_files = os.listdir(dedupe_input_dir)
file_dict = {}
for c in eu27:
    dedupe_file = [f for f in output_files if c == f[-6:-4]]
    if len(dedupe_file) > 0:
        file_dict[c] = dedupe_input_dir + dedupe_file[0]


def consolidate_unique(x):
    """
    Returns the first value in the series
    """
    return x.values[0]

def consolidate_set(x, delim='**', maxlen=100):
    """
    Consolidates all multi-valued strings in x
    into a unique set of maximum length maxlen.

    Returns a multivalued string separated by delim
    """
    out = delim.join(x.values)
    return out

for country in eu27:
    print country
    filename = raw_input_dir + 'cleaned_output_%s.tsv' % country.upper()
    print filename
    try:
        df = pd.read_csv(filename, sep='\t')
    except:
        continue

    df = df[['person_id', 'ipc_code']]

    df.ipc_code.fillna('', inplace=True)

    g = df.groupby('person_id')
    df_agg = g.agg({'ipc_code': consolidate_set})

    df_agg.reset_index(inplace=True)

    try:
        df_dedupe = pd.read_csv(file_dict[country])
    except:
        continue
    df_dedupe = df_dedupe[['Person', 'cluster_id', 'Name']]

    df_all = pd.merge(df_agg,
                      df_dedupe,
                      left_on='person_id',
                      right_on='Person',
                      how='inner'
                      )

    g = df_all.groupby('cluster_id')
    g_agg = g.agg({'Name': consolidate_unique,
                   'ipc_code': consolidate_set
                   }
                  )
    g_agg.reset_index(inplace=True)

    file_outname = output_dir + country + '_8dig_ipc.csv'
    g_agg.to_csv(file_outname, index=True)
