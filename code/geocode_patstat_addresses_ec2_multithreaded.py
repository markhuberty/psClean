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


class ThreadUrl(threading.Thread):
    """Threaded Url Grab"""
    def __init__(self, input_queue, output_queue, country, base_url):
        threading.Thread.__init__(self)
        self.input_queue = input_queue
        self.output_queue = output_queue
        self.country = country
        self.base_url = base_url


    def retrieve_url(self, url_string, max_errors):
        """
        Attempts to retrieve a url; traps http errors and retries until
        success or max_errors is reached. Returns result or None if error
        can't be resolved.
        """
        httperror = True
        error_count = 0
        http_result = None
        while httperror and error_count < max_errors:
            try:
                http_result = urllib2.urlopen(url_string)
                httperror = False
            except (urllib2.HTTPError, urllib2.URLError), e:
                try:
                    print e.reason
                except:
                    pass
                try:
                    print e.code
                except:
                    pass
                error_count += 1
                time.sleep(2)
        return http_result


    def encode_address(self, address_string):
        """
        Returns an encoded address string for the datasciencetoolkit
        Google-style geocoder
        """
        encode_dict = {'sensor': 'false',
                       'address': address_string
                       }
        encoded_address = urllib.urlencode(encode_dict)
        return encoded_address


    def format_latlng(self, address, json_output):
        """
        Returns a 3-tuple of address, lat, lng from a successfully geocoded address
        """
        formatted_latlng = (address,
                            json_output['results'][0]['geometry']['location']['lat'],
                            json_output['results'][0]['geometry']['location']['lng']
                            )
        return formatted_latlng

            
    def geocode(self, address):
        """
        Geocodes a single address, checking for whether null
        results go away with inclusion of country
        """

        if isinstance(address, str):
            
            
            encoded_address = self.encode_address(address)
            
            this_url = self.base_url + '%s' % encoded_address
            print this_url
            
            result = self.retrieve_url(this_url, 5)

            if result is not None:
                json_result = json.load(result)

                if json_result['status'] == "OK":
                    out = self.format_latlng(address, json_result)

                elif (json_result['status'] == "ZERO_RESULTS" and
                      self.country is not None):

                    encoded_address = self.encode_address(address + ' ' + self.country)

                    this_url = base_url + '%s' % encoded_address
                    print this_url

                    result = self.retrieve_url(this_url, 5)
                    if result is not None:
                        json_result = json.load(result)
                        if json_result['status'] == 'OK':
                            out = self.format_latlng(address, json_result)
                        else:
                            out = (address, None, None)
                    else:
                        out = (address, None, None)
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

def multithreaded_multiinstance_geocode(num_threads_per_instance,
                                        addresses,
                                        country,
                                        instance_urls):
    """
    Submits geocoding requests for every address in addresses. Requests are spread across
    num_threads_per_instance * len(instance_urls) threads. Each instance receives requests
    from only num_threads_per_instance threads. 
    """
    input_queue = Queue.Queue()
    output_queue = Queue.Queue()

    num_threads = num_threads_per_instance * len(instance_urls)
    thread_urls = instance_urls * num_threads_per_instance
    #spawn a pool of threads, and pass them queue instance 
    for i in range(num_threads):
        t = ThreadUrl(input_queue=input_queue,
                      output_queue=output_queue,
                      country=country,
                      base_url=thread_urls[i]
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
    if country_code in codes.values:
        long_country = countries[codes == country_code].values[0]
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
                                         iso_codes['country_name']
                                         )
    time_start = time.time()
    output = multithreaded_geocode(num_threads=2,
                                   addresses=df['person_address'].drop_duplicates().values,
                                   country=long_country,
                                   base_url=base_url
                                   )
    time_end = time.time()
    elapsed_time = time_end - time_start
    print 'Elapsed time: %s' % str(elapsed_time)
    df = merge(df, output, on='person_address', how='left')
    output_fname = 'geocoded_' + fname
    output.to_csv(output_fname)

ec2.terminate_instances(instance_ids=[r.instances[0].id])

