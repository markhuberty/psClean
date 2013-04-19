import pandas as pd
import re

re_name = re.compile('[0-9,]')

def parse_name(name):
    name_split = re_name.split(name, maxsplit=1)
    try:
        name_part, address_part = name_split
    except ValueError:
        name_part = name_split
        address_part = None

    return name_part, address_part


