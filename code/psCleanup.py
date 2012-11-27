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


def name_clean(input_string_list, cleanup_dicts):
    """
    Cycles through cleanup functions to return cleaned names.
    Args:
        inputlist: list of strings to be cleaned.
    Returns:
        outputlist: cleaned input string.
    """

    std_strings = [decoder(name) for name in input_string_list]
    std_strings = [remove_diacritics(s) for s in std_strings]
    std_strings = [stdize_case(s) for s in std_strings]
    output_string_list = master_clean_dicts(std_strings, cleanup_dicts)

    return output_string_list


def get_legal_ids(inputstring, legal_regex):
    """
    Small function to separate common legal identifiers from names.
    Args:
        inputstring: name string
    Returns:
        (name, ids): tuple of name and string of legal ids joined by '**' 
    """
    ids_list = legal_regex.findall(inputstring)
    ids = '**'.join(ids_list)
    name = legal_regex.sub('', inputstring)
    return name, ids


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

    n = min(len(comparisons), 10)
    comparison = '**'.join()comparisons[0:n])

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


legal_identifiers = """\\bBT\\b|\\bGMBH\\b|\\bPMDN\\b|\\bOYJ\\b|\\bEPE\\b|\\bRT\\b|\\bSGPS\\b|\\bPRC\\b|\\bOHG\\b|\\bRAS\\b|\\bSAS\\b|\\b\nSPA\\b|\\bKB\\b|\\bGIE\\b|\\bTD\\b|\\bPRP LTD\\b|\\bSNC\\b|\\bDBA\\b|\\bAPS\\b|\\bOE\\b|\\bA EN P\\b|\\bEXT\\b|\\b\nKAS\\b|\\bSCS\\b|\\bOY\\b|\\bSENC\\b|\\bAPB\\b|\\bOU\\b|\\bS DE RL\\b|\\bGBR\\b|\\bKOM SRK\\b|\\bHB\\b|\\bEEG\\b|\\b\nHF\\b|\\bLDC\\b|\\bSK\\b|\\bLDA\\b|\\bPT\\b|\\bLLP\\b|\\bSCA\\b|\\bEE\\b|\\bPTY\\b|\\bLLC\\b|\\bLTDA\\b|\\bSCP\\b|\\b\nPL\\b|\\bSOPARFI\\b|\\bEIRL\\b|\\bGCV\\b|\\bJTD\\b|\\bEV\\b|\\bCA\\b|\\bSA\\b|\\bVOF\\b|\\bSAICA\\b|\\bKKT\\b|\\b\nAVV\\b|\\bSAPA\\b|\\bSPRL\\b|\\bSPOL SRO\\b|\\bNA\\b|\\bINC\\b|\\bGESMBH\\b|\\bDOO\\b|\\bACE\\b|\\bKOL SRK\\b|\\b\nS EN C\\b|\\bKGAA\\b|\\bKDD\\b|\\bGMBH  CO KG\\b|\\bKDA\\b|\\bAPS  CO KS\\b|\\bASA\\b|\\bPMA\\b|\\bNT\\b|\\b\nDD\\b|\\bNV\\b|\\bTLS\\b|\\bSP ZOO\\b|\\bDNO\\b|\\bSRL\\b|\\bCORP\\b|\\bLTD\\b|\\bELP\\b|\\bEURL\\b|\\bCV\\b|\\b\nPC LTD\\b|\\bKG\\b|\\bSARL\\b|\\bKD\\b|\\bKK\\b|\\bSP\\b|\\bBV\\b|\\bKS\\b|\\bCVOA\\b|\\bPLC\\b|\\bKV\\b|\\bSC\\b|\\b\nKY\\b|\\bLTEE\\b|\\bBPK\\b|\\bIBC\\b|\\bDA\\b|\\bBVBA\\b|\\bCVA\\b|\\bKFT\\b|\\bSAFI\\b|\\bEOOD\\b|\\bSA DE CV\\b|\\b\nS EN NC\\b|\\bAMBA\\b|\\bSDN BHD\\b|\\bAC\\b|\\bAB\\b|\\bAE\\b|\\bAD\\b|\\bAG\\b|\\bIS\\b|\\bANS\\b|\\bAL\\b|\\bAS\\b|\\b\nOOD\\b|\\bVOS\\b|\\bVEB\\b"""



cleanup_dicts = get_dicts()
legal_regex = re.compile(legal_identifiers)
