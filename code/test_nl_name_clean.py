import pandas as pd
import time
import modifications


nl_data = pd.read_csv('/Users/markhuberty/Documents/Research/Papers/psClean/data/nl_test_data.csv')

nl_data = do_addresses(nl_data, 'NL')

nl_data = do_all(nl_data)
