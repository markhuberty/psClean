import pandas as pd
import MySQLdb
import pandas.io.sql as psql
import pdb


db=MySQLdb.connect(host='localhost',
                   port = 3306,
                   user='markhuberty',
                   passwd='patstat_huberty',
                   db = 'patstatOct2011'
                   )


def accumulate_naics(db_con):
    parent_query = """SELECT bvdep_id, naics_2007 FROM amadeus_parent_child
                      WHERE is_bvdep_id='nan' AND bvdep_id IN (SELECT is_bvdep_id FROM amadeus_parent_child WHERE is_bvdep_id!='nan')"""
    child_query = """SELECT bvdep_id, naics_2007 FROM amadeus_parent_child WHERE is_bvdep_id='%s'"""
    children_query = """SELECT bvdep_id, naics_2007 FROM amadeus_parent_child WHERE is_bvdep_id IN ('%s')"""
    #naics_query = """SELECT naics_2007 FROM amadeus_parent_child WHERE bvdep_id IN %s"""

    id_dict = {}
    no_parent = psql.frame_query(parent_query, con=db_con)

    #pdb.set_trace()
    
    print 'Ct parents: ', no_parent.shape
    for idx, id in enumerate(no_parent['bvdep_id']):

        if idx % 10000 == 0:
            print idx

        child = psql.frame_query(child_query % id, con=db_con)
        child_ids = child['bvdep_id']
        child_naics = child['naics_2007']
        # Recursively find all IDs in the tree
        #pdb.set_trace()
        
        if len(child_ids) > 0:
            id_dict[id] = {}
            id_list = []
            naics_list = []
            while True:
                id_list.extend(child_ids.values)
                naics_list.extend(child_naics.values)
                # print(len(id_list))
                children = psql.frame_query(children_query % "','".join(child_ids.values),
                                            con=db_con)
                # pdb.set_trace()
                if children.shape[0] == 0:
                    break
                # child = has_parent[has_children]
                child_ids = children.bvdep_id
                child_naics = children.naics_2007
            if len(id_list) > 0:
                id_dict[id]['child_ids'] = id_list
                id_dict[id]['child_naics'] = naics_list
            id_dict[id]['id_naics'] = no_parent[no_parent.bvdep_id==id]['naics_2007'].values[0]

    # for id in id_dict:
    #     if 'child_ids' in id_dict[id]:
    #         child_naics = psql.frame_query(naics_query % str(tuple(id_dict[id]['child_ids'])),
    #                                        con=db_con)
    #         id_dict[id]['child_naics'] = child_naics
    return id_dict

test = accumulate_naics(db)
