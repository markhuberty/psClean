import pandas as pd
import urllib2
import json
import boto ## for ec2 handling
import time
import numpy as np
import itertools as it
import os

os.chdir('/home/markhuberty/Documents/psClean/')

def retrieve_geocoded_response(url,  max_tries=3):

    httperror = True
    num_tries = 0
    while httperror and numtries < max_tries:
        try:
            response = urllib2.urlopen(url)
            httperror = False
        except(urllib2.HTTPError):
            response = None
            num_tries += 1

    return(response)
    

def geocode_address(address, country_name, root_url):
    param = '+'.join(address.split(' '))
    this_url = root_url + param
    print this_url

    url_response = retrieve_geocoded_response(this_url)
    
    if url_response:
        address = json.load(url_response)
        if address['status'] == 'ZERO_RESULTS':

            this_url = this_url + '+' + country_name
            url_response = retrieve_geocoded_response(this_url)

            if url_response:
                address = json.load(url_response)
                if address['status'] == 'ZERO_RESULTS':
                    lat, lng = None, None
                else:
                    lat = address['results'][0]['geometry']['location']['lat']
                    lng = address['results'][0]['geometry']['location']['lng']
            else:
                lat, lng = None, None

        else:
            lat = address['results'][0]['geometry']['location']['lat']
            lng = address['results'][0]['geometry']['location']['lng']
    else:
        lat, lng = None, None
    return (lat, lng)

## Generate the ec2 instance
access_id = ''
access_key = ''
ec2 = boto.connect_ec2(aws_access_key_id=access_id,
                       aws_secret_access_key=access_key
                       )

reservation = ec2.run_instances(image_id='ami-15449d7c',
                                key_name='huberty_ec2_key',
                                instance_type='m1.large',
                                security_groups=['dstk']
                                )
## Get console output:
reservations = ec2.get_all_instances()
instances = [i for r in reservations for i in r.instances]
this_instance = instances[-1]

time.sleep(180)

for r in ec2.get_all_instances():
    if r.id == reservation.id:
        break
this_instance = r.instances[0]            
dns_name = this_instance.public_dns_name
base_url = "http://" + dns_name + "/maps/api/geocode/json?sensor=false&address="

## Load the iso data
iso_codes = pd.read_csv('./data/iso_country_code_names.txt', sep=';',
                        names=['country_name', 'country_code'],
                        na_values=[],
                        )
iso_codes['country_code'][iso_codes['country_name']=='NAMIBIA'] = 'NA'

## Walk across the files and geocode non-blank addresses
datadir = './data/cleaned_data'
country_files = os.listdir(datadir) ## fix this
country_files = [f for f in country_files if 'NL' in f]


for f in country_files:
    fname = datadir + '/' + f
    df = pd.read_csv(fname)
    

    counter = 0
    latlng_list = []
    start_time = time.time()
    for addr, country in it.izip(df['person_address'], df['person_ctry_code']):
        if isinstance(addr, str) and len(addr) > 0:
            if country in iso_codes['country_code'].values:
                country_name = iso_codes['country_name'][iso_codes['country_code'] == country]
            else:
                country_name = ''

            latlng = geocode_address(addr, country_name.values[0], base_url)
        else:
            latlng = (None, None)
        latlng_list.append(latlng)
        
        counter += 1
        if counter > 0 and counter % 100 == 0:
            this_time = time.time()
            print 'Average query time:'
            txn_average =  (this_time - start_time) / counter
            print str(txn_average)
            
    df['lat'], df['lng'] = it.izip(*latlng_list)
    df.to_csv(fname)
 
ec2.terminate_instances(instance_ids=[this_instance.id])
