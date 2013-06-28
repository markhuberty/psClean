import pandas as pd

inputs = [i for idx, i in enumerate(sys.argv) if idx > 0]
root_dir = inputs[0]
output_dir = inputs[1]

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


def consolidate_set(x, delim='**', maxlen=100):
    """
    Consolidates all multi-valued strings in x
    into a unique set of maximum length maxlen.

    Returns a multivalued string separated by delim
    """
    vals = [v.split(delim) for v in x.values if isinstance(v, str)]
    val_set = [v for vset in vals for v in vset]
    val_set = list(set(val_set))
    if len(val_set) > 0:
        if len(val_set) > maxlen:
            rand_idx = random.sample(range(len(val_set)), maxlen)
            val_set = [val_set[idx] for idx in rand_idx]
        out = delim.join(val_set)
    else:
        out = ''
    return out

for country in eu27:
    print country
    filename = root_dir + country.upper() + '_cleaned_output.csv'
    try:
        df = pd.read_csv(filename, sep='\t')
    except:
        continue

    df = df[['person_id', 'ipc_code']]

    g = df.groupby('person_id')
    df_agg = g.agg(consolidate_set)

    file_outname = output_dir + country + '8dig_ipc.csv'
    df_agg.to_csv(file_outname, index=True)
