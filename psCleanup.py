# coding=utf8
############################
## Author: Mark Huberty, Mimi Tam, and Georg Zachmann
## Date Begun: 23 May 2012
## Purpose: Module to clean inventor / assignee data in the PATSTAT patent
##          database
## License: BSD Simplified
## Copyright (c) 2012, Authors
## All rights reserved.
##
## Redistribution and use in source and binary forms, with or without
## modification, are permitted provided that the following conditions are met: 
## 
## 1. Redistributions of source code must retain the above copyright notice, this
##    list of conditions and the following disclaimer. 
## 2. Redistributions in binary form must reproduce the above copyright notice,
##    this list of conditions and the following disclaimer in the documentation
##    and/or other materials provided with the distribution. 
## 
## THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
## ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
## WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
## DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR
## ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
## (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
## LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
## ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
## (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
## SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
## 
## The views and conclusions contained in the software and documentation are those
## of the authors and should not be interpreted as representing official policies, 
## either expressed or implied, of the FreeBSD Project.
############################

import re
import unicodedata
import string
import pickle


def stdize_case(mystring): 
    result = mystring.upper()
    return result

def remove_diacritics(inputstring): 
    """
    Small function to remove diacritics/accents and ensure utf8 encoding.
    Args:
        inputstring: string
    Returns:
        returns the input string in utf8 encoding with diacritics removed.
    """
    nkfd_form = unicodedata.normalize('NFKD', unicode(inputstring))
    only_ascii = u"".join([c for c in nkfd_form if not unicodedata.combining(c)])
    outputstring = only_ascii.encode('utf8')
    return outputstring


def mult_replace(input_string, regex_dict):
    """
    Function to replace input_string with different values using a
    a regex_dict.
    Args:
        input_string: string to be cleaned
        regex_dict: dict of regular expressions used to clean the input_string 
    Returns:
        output_string: cleaned string (using the regex_dict)
    """
    output_string = input_string
    for k, v in regex_dict.iteritems():
        output_string = v.sub(k, output_string)
    return output_string


def make_regex(input_dict):
    """
    Function to create a regex_dict from an input dict of regular expressions
    Args:
        input_dict: dict of regular expressions to be compiled
    Returns:
        regex_dict: dict of compiled regular expressions
    """
    regex_dict = {}
    for k, v in input_dict.iteritems():
        if isinstance(v, str):
            regex_dict[k] = re.compile(v)
        elif isinstance(v, list):
            expression = '|'.join(v)
            regex_dict[k] = re.compile(expression, re.UNICODE)
        else:
            raise Exception('Invalid input type!')## Throw an error
    return regex_dict


def master_clean_dicts(input_string_list, cleanup_dicts):
    """
    Checks each string in an list and does a find/replace based on
    values in a list of replace:find dicts
    
    Args:
        input_string_list: string to be cleaned
        cleanup_dicts: list of dicts to be used on the input string.
        Dicts should be replace:find where find is either a string or
        a list of strings
    Returns:
    A list of cleaned strings of same length as input_string_list
    """
    regex_dicts = [make_regex(d) for d in cleanup_dicts]
    
    for i, s in enumerate(input_string_list):
        for cleanup_dict in regex_dicts:
            s = mult_replace(s, cleanup_dict)
        input_string_list[i] = s
    return input_string_list   


def ipc_clean(codes):
    """
    Small function to strip ipc field of spaces and punctuation.
    Args:
        codes: list of ipc codes.
    Returns:
        codes: stripped ipc codes.
    """
    codes = [re.sub(r"\s|/", '', item) for item in codes]
    return codes


def name_clean(inputlist):
    """
    Cycles through cleanup functions to return cleaned names.
    Args:
        inputlist: list of strings to be cleaned.
    Returns:
        outputlist: cleaned input string.
    """
    for i, mystring in enumerate(inputlist):
        a = decoder(mystring)
        b = remove_diacritics(a)
        c = stdize_case(b)
        inputlist[i] = c
    clean_strings = master_clean_dicts(inputlist, cleanup_dics)
    outputlist = [encoder(v) for v in clean_strings]
    return outputlist


def get_legal_ids(inputstring):
    """
    Small function to separate common legal identifiers from names.
    Args:
        inputstring: name string
    Returns:
        (name, ids): tuple of name and string of legal ids joined by '**' 
    """
    v = re.compile(legal_identifiers)   
    ids_list = v.findall(inputstring)
    ids = '**'.join(ids_list)
    name = v.sub('', inputstring)
    return (name, ids)


def encoder(v):
    """
    Small function to encode only the strings to UTF-8.
    Used mainly to convert items in rows to utf-8 before being written to
    csv by csv.writer which does not accept unicode.
    Args:
        v: any type
    Returns:
        v as utf-8 if it was unicode, otherwise return v
    Usage(in context of rows):
        for row in results:
            writer.writerow(tuple(encoder(v) for i,v in enumerate(row)))
    """
    if isinstance(v,unicode):
        return v.encode('utf-8')
    else:
        return v


def decoder(v,encoding='latin1'):
    """
    Small function to decode for csv reader. Needed to not decode longs.
    Args:
        v: utf-8
    Returns:
        v as unicode if it was utf-8, otherwise return v
    Usage (in context of a csv reader):
        for row in reader:
            decoded.append([decoder(cell) for cell in row])
    """
    if isinstance(v,basestring):
        if not isinstance(v,unicode):
            return unicode(v,encoding)

        
def get_max(comparisons):
    """
    Small function to limit the length of coauthor and ipc fields to 10.
    Args:
        list of cleaned coauthors or ipc codes
    Returns:
        string of authors or ipc codes up to a maximum of 10.
    """

    n = len(comparisons)
    if n > 10:
        max_comparisons = comparisons[0:9]
    else:
        max_comparisons = comparisons[0:n]
    comparison = '**'.join(max_comparisons)
    return comparison


def get_dicts():
    """
    Function to unpickle large dictionaries and collect smalller ones.
    Args:
        None
    Returns:
        List of dictionaries to be used in cleaning.
    """
    dics_list = ['abbreviations', 'us_uk']
    all_dics = [ampersand, clean_symbols, convert_sgml, single_space]

    some_dics = {}
    
    for dictionary in dics_list:
        with open(dictionary, 'r') as f:
            some_dics[dictionary] = pickle.load(f)

    all_dics.extend(some_dics.values())
    return all_dics


#Dictionaries used for cleaning and list of legal identifiers.
#IMPORTANT NOTE: These all assume that case standardization has already been 
#performed!


convert_html = {
    ' ': r'<\s*BR\s*>' #break 
    }

convert_sgml = {
    '': r'&AMP;|&OACUTE;|&SECT;|&UACUTE;|&#8902;|&BULL;|&EXCL;'
    }

clean_symbols = {
    '': r'[^\s\w]'
    }

single_space = {
    ' ':r'\s+' 
    }

#translate "ands" in common languages into the ampersand symbol. 
ampersand = {
    '&' : [r'(?<=\s)AND(?=\s)',
           r'(?<=\s)EN(?=\s)',
           r'(?<=\s)DHE(?=\s)',
           r'(?<=\s)və(?=\s)',
           r'(?<=\s)ETA(?=\s)',
           r'(?<=\s)I(?=\s)',
           r'(?<=\s)и(?=\s)',
           r'(?<=\s)A(?=\s)',
           r'(?<=\s)OG(?=\s)',
           r'(?<=\s)KAJ(?=\s)',
           r'(?<=\s)JA(?=\s)',
           r'(?<=\s)AT(?=\s)',
           r'(?<=\s)ET(?=\s)',
           r'(?<=\s)E(?=\s)',
           r'(?<=\s)UND(?=\s)',
           r'(?<=\s)AK(?=\s)',
           r'(?<=\s)ES(?=\s)',
           r'(?<=\s)DAN(?=\s)',
           r'(?<=\s)AGUS(?=\s)',
           r'(?<=\s)UN(?=\s)',
           r'(?<=\s)IR(?=\s)',
           r'(?<=\s)U\s',
           r'(?<=\s)SI(?=\s)',
           r'(?<=\s)IN(?=\s)',
           r'(?<=\s)Y(?=\s)',
           r'(?<=\s)NA(?=\s)',
           r'(?<=\s)OCH(?=\s)',
           r'(?<=\s)VE(?=\s)',
           r'(?<=\s)VA(?=\s)',
           r'(?<=\s)SAMT(?=\s)'
           ] 
    }

legal_identifiers = ("""BT\\b|GMBH\\b|PMDN\\b|OYJ\\b|EPE\\b|RT\\b|SGPS\\b|PRC\\b|OHG\\b|RAS\\b|SAS\\b|
SPA\\b|KB\\b|GIE\\b|TD\\b|PRP LTD\\b|SNC\\b|DBA\\b|APS\\b|OE\\b|A EN P\\b|EXT\\b|
KAS\\b|SCS\\b|OY\\b|SENC\\b|APB\\b|OU\\b|S DE RL\\b|GBR\\b|KOM SRK\\b|HB\\b|EEG\\b|
HF\\b|LDC\\b|SK\\b|LDA\\b|PT\\b|LLP\\b|SCA\\b|EE\\b|PTY\\b|LLC\\b|LTDA\\b|SCP\\b|
PL\\b|SOPARFI\\b|EIRL\\b|GCV\\b|JTD\\b|EV\\b|CA\\b|SA\\b|VOF\\b|SAICA\\b|KKT\\b|
AVV\\b|SAPA\\b|SPRL\\b|SPOL SRO\\b|NA\\b|INC\\b|GESMBH\\b|DOO\\b|ACE\\b|KOL SRK\\b|
S EN C\\b|KGAA\\b|KDD\\b|GMBH  CO KG\\b|KDA\\b|APS  CO KS\\b|ASA\\b|PMA\\b|NT\\b|
DD\\b|NV\\b|TLS\\b|SP ZOO\\b|DNO\\b|SRL\\b|CORP\\b|LTD\\b|ELP\\b|EURL\\b|CV\\b|
PC LTD\\b|KG\\b|SARL\\b|KD\\b|KK\\b|SP\\b|BV\\b|KS\\b|CVOA\\b|PLC\\b|KV\\b|SC\\b|
KY\\b|LTEE\\b|BPK\\b|IBC\\b|DA\\b|BVBA\\b|CVA\\b|KFT\\b|SAFI\\b|EOOD\\b|SA DE CV\\b|
S EN NC\\b|AMBA\\b|SDN BHD\\b|AC\\b|AB\\b|AE\\b|AD\\b|AG\\b|IS\\b|ANS\\b|AL\\b|AS\\b|
OOD\\b|VOS\\b|VEB\\b""")

cleanup_dics = get_dicts()
