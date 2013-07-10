import urllib
import bs4

url = 'http://www.corporateinformation.com/Company-Extensions-Security-Identifiers.aspx'

con = urllib.urlopen(url)
doc = con.read()
con.close()


soup = bs4.BeautifulSoup(doc, 'lxml')
tables = soup.find_all('table')


records = []
for t in tables:
    inner_tables = t.find_all('table')
    if len(inner_tables) > 0:
        table_record = []
        for it in inner_tables:
            rows = it.find_all('tr')
            for r in rows:
                cols = r.find_all('td')
                col_record = []
                for c in cols:
                    if c.i:
                        col_record.append(c.i.string)
                    else:
                        if c.string and len(c.string.strip()) > 0:
                            col_record.append(c.string.strip())
                    if len(col_record) > 0:
                        table_record.append(col_record)
        records.append(table_record)

# This is a hack, but it works.
company_table = records[-2][:-33]

import pandas as pd
df = pd.DataFrame(company_table)
df.columns = ['code', 'country', 'description']
df = df.drop_duplicates()

# Strip out some weird diacritics
import unidecode
codes_uni = [unidecode.unidecode(c) for c in df.code]
descriptions_uni = [unidecode.unidecode(d) if d else '' for d in df.description]

df['codes_ascii'] = codes_uni
df['description_ascii'] = descriptions_uni

df.to_csv('company_legal_ids.csv', index=False, encoding='utf-8')
