import pandas as pd

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



citl_file_root = '../../data/citl/%scode.csv'
country_files = []

cols = ['identifierinreg',
        'registrycode',
        'accountholder',
        'installationidentifier',
        'name',
        'countrycode',
        'mainactivitytypecodelookup'
        ]

for idx, country in enumerate(eu27):
    try:
        df = pd.read_csv(citl_file_root % country.upper())
    except:
        continue
    df = df[cols]
    country_files.append(df)

df_out = pd.concat(country_files, ignore_index=True)
df_out.to_csv('../../data/citl/citl_eu27.csv', index=False)                     
