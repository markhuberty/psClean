import Queue
import httplib
import json
import os
import pandas as pd
import threading
import time
import urllib
import urllib2


class ThreadUrl(threading.Thread):
    """Threaded Url Grab"""
    def __init__(self, input_queue, output_queue, country, base_url):
        threading.Thread.__init__(self)
        self.input_queue = input_queue
        self.output_queue = output_queue
        self.country = country
        self.base_url = base_url


    def geocode(self, address):
        """
        Geocodes a single address, checking for whether null
        results go away withinclusion of country
        """

        if isinstance(address, str):

            encode_dict = {'sensor': 'false',
                           'address': address
                           }
            encoded_address = urllib.urlencode(encode_dict)
            
            this_url = self.base_url + '%s' % encoded_address
            
            result = urllib2.urlopen(this_url)
            json_result = json.load(result)

            if json_result['status'] == "OK":
                out = (address,
                       json_result['results'][0]['geometry']['location']['lat'],
                       json_result['results'][0]['geometry']['location']['lng']
                       )
                
            elif json_result['status'] == "ZERO_RESULTS" and self.country is not None:
                encode_dict = {'sensor': 'false',
                               'address': address + ' ' + self.country
                               }
                encoded_address = urllib.urlencode(encode_dict)
                
                this_url = base_url + '%s' % encoded_address

                    
                result = urllib2.urlopen(this_url)
                json_result = json.load(result)
                if json_result['status'] == 'OK':
                    lat = json_result['results'][0]['geometry']['location']['lat']
                    lng = json_result['results'][0]['geometry']['location']['lng']
                    out = (address,
                           lat,
                           lng
                           )
                else:
                    out = (address, None, None)

            else:
                out = (address, None, None)
        else:
            out = (address, None, None)
        return(out)

          
    def run(self):

        while True:
            #grabs address from queue

            address = self.input_queue.get()
            out = self.geocode(address)

            self.output_queue.put(out)                
            #signals to queue job is done
            self.input_queue.task_done()
          

def multithreaded_geocode(num_threads,
                          addresses,
                          country,
                          base_url):
    """
    Submits geocoding requests for every address in addresses, across num_threads
    threads, to a geocoder at base_url.
    """
    input_queue = Queue.Queue()
    output_queue = Queue.Queue()
    
    #spawn a pool of threads, and pass them queue instance 
    for i in range(num_threads):
        t = ThreadUrl(input_queue=input_queue,
                      output_queue=output_queue,
                      country=country,
                      base_url=base_url
                      )
        t.setDaemon(True)
        t.start()
              
    #populate queue with data   
    for address in addresses:
        input_queue.put(address)
           
    #wait on the queue until everything has been processed     
    input_queue.join()

    print 'Input queue exhausted, extracting results'
    ## Extract the queue data
    results = []
    for idx in range(output_queue.qsize()):
        result = output_queue.get()
        results.append(result)

    results_df = pd.DataFrame.from_records(results,
                                           columns=['person_address',
                                                    'lat',
                                                    'lng'
                                                    ]
                                           )
    return(results_df)


def short_to_long_country(country_code, codes, countries):
    """
    Utility function to crosswalk a country code to a country name
    Assumes codes and countries are pandas Series objects of the same
    length
    """
    if country_code in codes:
        long_country = countries[codes == country_code][0]
    else:
        long_country = None
    return long_country



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
## Give it time to boot
time.sleep(60)

for r in ec2.get_all_instances():
    if r.id == reservation.id:
        break
this_instance = r.instances[0]            
dns_name = this_instance.public_dns_name

## Set the directory root and dstk URL
os.chdir('/home/markhuberty/Documents/psClean/')


#base_url = 'http://www.datasciencetoolkit.org/maps/api/geocode/json?'
base_url = "http://" + dns_name + "/maps/api/geocode/json?"



## Grab the ISO code/country crosswalk table
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
    this_country = df['person_ctry_code'].values[0]
    long_country = short_to_long_country(this_country,
                                         iso_codes['country_code'],
                                         iso_codes['contry_name']
                                         )
    output = multithreaded_geocode(num_threads=10,
                                   df['person_address'].values,
                                   country=long_country,
                                   base_url=base_url
                                   )
    output_fname = 'geocoded_' + fname
    output.to_csv(output_fname)


