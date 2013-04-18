import Queue
import boto
import httplib
import json
import os
import pandas as pd
import threading
import time
import urllib
import urllib2
from dstk_ec2_geocoder import *

## Set the directory root and dstk URL
os.chdir('/home/markhuberty/Documents/psClean/')


## Generate the ec2 instance
ec2_credentials = pd.read_csv('./data/ec2_access_credentials.csv')
access_id = ec2_credentials['access_id'].values[0]
access_key = ec2_credentials['secret_access_code'].values[0]
ec2 = boto.connect_ec2(aws_access_key_id=access_id,
                       aws_secret_access_key=access_key
                       )

reservation = ec2.run_instances(image_id='ami-15449d7c',
                                key_name='huberty_ec2_key_2',
                                instance_type='m1.large',
                                security_groups=['dstk']
                                )

## Get console output:
reservations = ec2.get_all_instances()
instances = [i for r in reservations for i in r.instances]
this_instance = instances[-1]

## Give it time to boot
instance_check_interval = 30
instance_boot_time = 0
while this_instance.state == u'pending':
    print this_instance.state
    time.sleep(instance_check_interval)
    instance_boot_time += instance_check_interval
    this_instance.update()



for r in ec2.get_all_instances():
    if r.id == reservation.id:
        break
this_instance = r.instances[0]
print this_instance.state
dns_name = this_instance.public_dns_name
time.sleep(120)




#base_url = 'http://www.datasciencetoolkit.org/maps/api/geocode/json?'
base_url = "http://" + dns_name + "/maps/api/geocode/json?"



## Grab the ISO code/country crosswalk table
iso_codes = pd.read_csv('./data/iso_country_code_names.txt', sep=';',
                        names=['country_name', 'country_code'],
                        na_values=[],
                        )
iso_codes['country_code'][iso_codes['country_name']=='NAMIBIA'] = 'NA'

## Walk across the files and geocode non-blank addresses
datadir = '/mnt/db_master/patstat_raw/fleming_inputs/'
#datadir = './data/cleaned_data'
country_files = os.listdir(datadir) ## fix this
country_files = [f for f in country_files if f == 'cleaned_output_DE.tsv']
    

for f in country_files:
    fname = datadir + '/' + f
    df = pd.read_csv(fname, sep='\t')
    df = df[['person_id', 'person_address', 'person_ctry_code']].dropna().drop_duplicates()
    this_country = df['person_ctry_code'].values[0]
    long_country = short_to_long_country(this_country,
                                         iso_codes['country_code'],
                                         iso_codes['country_name']
                                         )
    addresses = df['person_address'].drop_duplicates().values
    time_start = time.time()
    output = multithreaded_geocode(num_threads=2,
                                   addresses=addresses,
                                   country=long_country,
                                   base_url=base_url,
                                   ec2_instance=this_instance
                                   )
    time_end = time.time()
    elapsed_time = time_end - time_start
    print 'Elapsed time: %s' % str(elapsed_time)
    df = pd.merge(df, output, on='person_address', how='left')
    output_fname = datadir + '/geocoded_' + f
    df.to_csv(output_fname, cols=['person_id', 'person_address'])

ec2.terminate_instances(instance_ids=[r.instances[0].id])

