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

def unroll_all_naics(naics_accum, generics):
    new_naics_assignments = []
    for id in naics_accum:
        old_naics = naics_accum[id]['id_naics']
        new_naics = pd.Series(naics_accum[id]['child_naics'])
        
        new_naics = new_naics[new_naics > 0]
        new_naics.append(pd.Series([old_naics]))

        naics_counts = new_naics.value_counts()
        id_vec = [id] * len(naics_counts)

        temp = zip(id_vec, naics_counts.index, naics_counts.values)
        new_naics_assignments.extend(id_vec)
        for cid in naics_accum[id]['child_ids']:
            cid_vec = [cid] * len(naics_counts)
            new_naics_assignments.extend(zip(cid_vec, naics_counts.index, naics_counts.values))
    return new_naics_assignments

all_unrolled = unroll_all_naics(test, [])
all_unrolled = [n for n in all_unrolled if len(n) == 3]
df_unrolled = pd.DataFrame(all_unrolled, columns=['amadeus_id', 'naics', 'naics_ct'])
df_unrolled.to_csv('amadeus_naics_map.csv', index=False)


def unroll_naics(naics_accum, generics):
    new_naics_assignments = []
    for id in naics_accum:
        old_naics = naics_accum[id]['id_naics']
        new_naics = pd.Series(naics_accum[id]['child_naics'])
        
        new_naics = new_naics[new_naics > 0]

        is_generic = new_naics.isin(generics)

        if not all(is_generic):
            new_naics = new_naics[~is_generic]

        naics_counts = new_naics.value_counts()

        if len(naics_counts) > 0:
            max_naics = naics_counts.index[0]
        else:
            max_naics = old_naics
        new_naics_assignments.append((id, max_naics))
        for cid in naics_accum[id]['child_ids']:
            new_naics_assignments.append((cid, max_naics))
    return new_naics_assignments
                                                                                
test_unroll = unroll_naics(test, [5511, 5411, 5239, 5311])

df_test_unroll = pd.DataFrame(test_unroll, columns=['bvdep_id', 'new_naics'])

insert_query = """UPDATE amadeus_parent_child SET new_naics=%d WHERE bvdep_id='%s'"""

db_cursor = db.cursor()
for idx, row in df_test_unroll.iterrows():
    db_cursor.execute(insert_query % (row['new_naics'], row['bvdep_id']))
