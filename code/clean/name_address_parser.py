import pandas as pd
import re
import math

re_name = re.compile('(?<=\w)\s+[0-9]{2,}\s+(?=\w)')
re_test = re.compile('\s+[0-9]{2,}\s+(?=\w)')

def parse_name(name):
    name_split = re_name.split(name, maxsplit=1)
    try:
        name_part, address_part = name_split
    except ValueError:
        name_part = name_split[0]
        address_part = None

    return name_part, address_part

def filter_name(name):
    name_sub_idx = int(math.ceil(len(name) / 2.0))
    name_sub = name[-name_sub_idx:]
    if re_test.search(name_sub):
        return True
    return False
