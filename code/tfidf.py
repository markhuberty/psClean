def tfidf(strlist, threshold=2):
    term_dict = {}
    for s in strlist:
        if isinstance(s, str):
            s_split = s.split(' ')
            for ss in s_split:
                if len(ss) > threshold:
                    if ss in term_dict:
                        term_dict[ss] += 1
                    else:
                        term_dict[ss] = 1
    return term_dict
