import pandas as pd
import re
import math

"""
These functions attempt to intelligently parse names out of addresses for instances
where a PATSTAT name contains an address field. They assume that an address
is distinguished by two or more numbers in a name, bounded on either side by
words and spaces.

E.g., this is an address: Jon Doe, 1234 Bond St, Boston, MA, US
      this is not an address: Jon Doe, Boston

filter_name first searches for possible addresses in the
latter half of the string.
"""

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
