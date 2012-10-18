#import MySQLdb
import csv
from nav_query_results import *
import re
from collections import defaultdict
from psCleanup import *
import itertools


"""
db = MySQLdb.connect(host="localhost", port=3306, user="mimitam", passwd="tam_patstat2011", db="patstatOct2011", use_unicode=True, charset='utf8')
cursor = db.cursor()

#The correct query - corrected for # of applicants - to be summed by country
query = '''
SELECT t1.person_ctry_code, t1.ipc_class_symbol, t1.appln_id, numerator / divisor
FROM
(SELECT person_ctry_code, ipc_class_symbol, appln_id, count(*) as numerator 
FROM tls209_appln_ipc INNER JOIN (tls207_pers_appln INNER JOIN tls206_person USING (person_id)) USING (appln_id)
GROUP BY person_ctry_code, ipc_class_symbol, appln_id
) t1
JOIN 
(SELECT appln_id, count(person_id) as divisor
FROM tls207_pers_appln
GROUP BY appln_id
) t2 using(appln_id)
'''
"""
    


###1.
#First find the denominator which is the count of persons per application id
fieldnames = ['person_id',
              'appln_id',
              'applt_seq_nr',
              'invt_seq_nr']

namefile_path = '/mnt/db_master/patstat_raw/dvd1'

namefiles = ['tls207_part0' + str(i) + '.txt' for i in range(1,6)]

#Rolling count of person_ids for each appln_id
#Note that in the higher versions of python, i.e. 2.7 or above, can use subclass "Counter"
applnid_perscount = defaultdict(int)

for f in namefiles:
    full_path = namefile_path + '/' + f
    g = open(full_path,'rt')
    reader = csv.DictReader(g,fieldnames=fieldnames)
    for row in reader:
        applnid_perscount[row['appln_id']] +=1

#store this to file and delete to conserve memory
f = open('appln_pers_count.csv','wb')
writer = csv.writer(f)
values = [v for k,v in applnid_perscount.iteritems()]
#Not the ideal way to write files but with dictwriter I had some trouble
#So I am writing these as columns and will need to turn the values froms tring
#back to int
writer.writerows(itertools.izip(*[applnid_perscount.keys(),values]))
f.close()
#del applnid_perscount

###2.
#Since it will be a join of 3 tables, we need to do multiple steps for the numerator
#In order to make sure we don't run out of memory we can do 10mil applications at a time
#First get the number of applications
db = MySQLdb.connect(host="localhost", port=3306, user="mimitam", passwd="tam_patstat2011", db="patstatOct2011", use_unicode=True, charset='utf8')
cursor = db.cursor()
cursor.execute('select count(appln_id) from tls201_appln')
num_appln = cursor.fetchall()[0]
cursor.close()
db.close()

main_count = 0
#set the limit for each iteration
limit = 10000000
##2a. Get person_ids and the country codes associated with them

#use a set to track how many applns have already had the count done
set_applns = set()

while main_count < num_appln[0]:
    fieldnames = ['person_id',
                  'appln_id',
                  'applt_seq_nr',
                  'invt_seq_nr']
    
    namefile_path = '/mnt/db_master/patstat_raw/dvd1'

    namefiles = ['tls207_part0' + str(i) + '.txt' for i in range(1,6)]

    #Rolling count of person_ids for each appln_id
    #Note that in the higher versions of python, i.e. 2.7 or above, can use subclass "Counter"
    applnid_persid = defaultdict(list)
    
    #to track the unique personids associated with these applications
    set_persids = set()

    for f in namefiles:
        full_path = namefile_path + '/' + f
        g = open(full_path,'rt')
        reader = csv.DictReader(g,fieldnames=fieldnames)
        key_counter = 0
        for row in reader:
            #check if max number of keys exceeded and whether the appln was counted
            #in a previous iteration
            if key_counter < limit & row['appln_id'] not in set_applns: 
                applnid_persid[row['appln_id']].append(row['person_id'])
                set_persids.add(row['person_id'])
                key_counter+=1  
            else:
                if row['appln_id'] in applnid_persid.keys():
                    applnid_persid[row['appln_id']].append(row['person_id'])
                    set_persids.add(row['person_id'])
                    key_counter+=1
        g.close()   
    
    #now get the country codes attached to relevant personids to this search
    fieldnames = ['person_id',
                  'person_ctry_code',
                  'doc_std_name_id',
                  'person_name',
                  'person_address'
                  ]

    namefile_path = '/mnt/db_master/patstat_raw/dvd1'
    namefiles = ['tls206_part0' + str(i) + '_clean.txt' for i in range(1,6)]

    persid_ctry = defaultdict(str)
    for f in namefiles:
        full_path = namefile_path + '/' + f
        g = open(full_path,'rt')
        reader = csv.DictReader(g,fieldnames=fieldnames)
        for row in reader:
            if row['person_id'] in set_persids:
                persid_ctry[row['person_id']] = row['person_ctry_code']
    g.close()
    
    #now create the appln_id-country count
    applnctry_count = defaultdict(int)
    for appln_id in applnid_persid.keys():
        for person_id in applnid_persid[appln_id]:
            applnctry_count[appln_id + persid_ctry[person_id]] +=1
    
    #Don't need personids anymore so delete these vars
    del set_persids
    del persid_ctry
    
    #only need the keys now then delete; use set operations
    set_applns_temp = set(applnid_persid.keys())
    
    #update applications already counted
    set_applns.add(set(applnid_persid.keys()))
    del applnid_persid
    
    #Now need to get the IPC codes for each applnid
    fieldnames = ['appln_id',
                  'ipc_class_symbol',
                  'ipc_class_level',
                  'ipc_version',
                  'ipc_value',
                  'ipc_position',
                  'ipc_gener_auth']
    
    namefile_path = '/mnt/db_master/patstat_raw/dvd1'
    namefiles = ['tls209_part0' + str(i) + '.txt' for i in range(1,8)]
    
    applnctry_ipc = defaultdict(int)
    
    for f in namefiles:
        full_path = namefile_path + '/' + f
        g = open(full_path,'rt')
        reader = csv.DictReader(g,fieldnames=fieldnames)
        for row in reader:
            this_ipc = single_space.sub(' ',rem_trail_spaces(row['ipc_class_symbol']))
            if row['appln_id'] in set_applns_temp:
                for applnctry,c in applnctry_count.iteritems():
                    #Country codes are only 2 letters long
                    appln = applnctry[:-2]
                    ctry = applnctry[-2:]
                    applnctry_ipc[row[appln] + this_ipc + ctry] += c

    applnctryipc_count = defaultdict(int)
    #Can do the division directly
    for applnctryipc,v in applnctry_ipc.iteritems():
        #Should double check this! Applications appear to be 9 digits
        appln = applnctry[:8]
        applnctryipc_count[applnctryipc] = v/applnid_perscount[appln]
                

    #increment the counter
    main_count = main_count + limit
    #write to file before starting loop over ('a' option means to append)
    h = open('applnipcctry_count.csv','ab')
    writer = csv.writer(f)
    values = [v for k,v in applnctryipc_count.iteritems()]
    #Not the ideal way to write files but with dictwriter I had some trouble
    #So I am writing these as columns and will need to turn the values froms tring
    #back to int
    writer.writerows(itertools.izip(*[applnctryipc_count.keys(),values]))
    h.close()
    del applnctry_ipc
    del applnctry_count


