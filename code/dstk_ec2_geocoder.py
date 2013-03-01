import Queue
import boto
import httplib
import json
import pandas as pd
import threading
import time
import urllib
import urllib2

## Class to handle the multithreaded geocoder operations.
class ThreadUrl(threading.Thread):
    """Threaded Url Grab"""
    def __init__(self,
                 input_queue,
                 output_queue,
                 country,
                 base_url,
                 ec2i,
                 server_status_event):
        threading.Thread.__init__(self)
        self.input_queue = input_queue
        self.output_queue = output_queue
        self.country = country
        self.base_url = base_url
        self.ec2_instance = ec2i
        self.server_status_event = server_status_event

    def reboot_ec2_instance(self, instance, server_isdown,
                            reboot_check_interval=10):
        """
        Check if the server event status is set. If not,
        reboot the ec2 instance; otherwise wait
        until the event is unset and return.
        """
        if server_isdown.isSet():
            print 'awaiting server reboot'
            while server_isdown.isSet():
                continue
        else:
            print 'server error, rebooting'
            print threading.current_thread()
            server_isdown.set()
            ec2boot = instance.reboot()
            time.sleep(5)
            while instance.state == u'pending':
                time.sleep(reboot_check_interval)
                instance.update()
            ## Give the server enough time to come up fully
            time.sleep(60)
            print 'reboot complete'
            server_isdown.clear()
        return True

    def retrieve_url(self, url_string, max_errors,
                     ec2_instance, server_status_event):
        """
        Attempts to retrieve a url; traps http error.
        For error code 500, reboots the ec2 instance. Otherwise,
        retries until success or max_errors is reached. Returns
        result or None if error can't be resolved.
        """
        httperror = True
        error_count = 0
        http_result = None
        while httperror and error_count < max_errors:
            try:
                http_result = urllib2.urlopen(url_string)
                httperror = False
            except (urllib2.HTTPError, urllib2.URLError,
                    httplib.BadStatusLine), e:
                try:
                    if e.code == 500:
                        self.reboot_ec2_instance(ec2_instance,
                                                 server_status_event
                                                 )
                except:
                    pass
                try:
                    if e.reason and server_status_event.isSet():
                        print 'awaiting server reboot'
                        while server_status_event.isSet():
                            continue
                except:
                    print e
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

    def extract_locality(self, json_output):
        addr_comp = json_output['results'][0]['address_components']
        locality = None
        for element in addr_comp:
            if 'locality' in element['types']:
                locality = element['long_name']
            else:
                continue
        return locality

    def format_latlng(self, address, json_output):
        """
        Returns a 4-tuple of address, lat, lng, locality
        from a successfully geocoded address
        """
        lat = json_output['results'][0]['geometry']['location']['lat']
        lng = json_output['results'][0]['geometry']['location']['lng']
        formatted_latlng = (address,
                            lat,
                            lng,
                            self.extract_locality(json_output)
                            )
        return formatted_latlng

    def geocode(self, address, ec2_instance, server_status_event):
        """
        Geocodes a single address, checking for whether null
        results go away with inclusion of country
        """

        if isinstance(address, str):
            encoded_address = self.encode_address(address)

            this_url = self.base_url + '%s' % encoded_address
            #print this_url

            result = self.retrieve_url(this_url,
                                       5,
                                       ec2_instance, server_status_event
                                       )

            if result is not None:
                json_result = json.load(result)

                if json_result['status'] == "OK":
                    out = self.format_latlng(address, json_result)

                elif (json_result['status'] == "ZERO_RESULTS" and
                      self.country is not None):

                    addr = address + ' ' + self.country
                    encoded_address = self.encode_address(addr)

                    this_url = base_url + '%s' % encoded_address
                    #print this_url

                    result = self.retrieve_url(this_url, 5, ec2_instance, server_status_event)
                    if result is not None:
                        json_result = json.load(result)
                        if json_result['status'] == 'OK':
                            out = self.format_latlng(address, json_result)
                        else:
                            out = (address, None, None, None)
                    else:
                        out = (address, None, None, None)
                else:
                    out = (address, None, None, None)
            else:
                out = (address, None, None, None)
        else:
            out = (address, None, None, None)
        print out
        return(out)

          
    def run(self):
        orig_input_queue_len = self.input_queue.qsize()
        while True:
            #grabs address from queue

            address = self.input_queue.get()
            out = self.geocode(address, self.ec2_instance, self.server_status_event)
            
            self.output_queue.put(out)
            # if self.input_queue.qsize() % 1000 == 0:
            #     print '% percent complete' % self.output_queue.qsize() / float(orig_input_queue_len)
            #signals to queue job is done
            self.input_queue.task_done()

## And wrapper / helper functions
def multithreaded_geocode(num_threads,
                          addresses,
                          country,
                          base_url,
                          ec2_instance):
    """
    Submits geocoding requests for every address in addresses, across num_threads
    threads, to a geocoder at base_url.
    """
    input_queue = Queue.Queue()
    output_queue = Queue.Queue()
    server_status_event = threading.Event()
    
    #spawn a pool of threads, and pass them queue instance 
    for i in range(num_threads):
        t = ThreadUrl(input_queue=input_queue,
                      output_queue=output_queue,
                      country=country,
                      base_url=base_url,
                      ec2i=ec2_instance,
                      server_status_event=server_status_event
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
                                                    'lng',
                                                    'locality'
                                                    ]
                                           )
    return results_df

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
                                                    'lng',
                                                    'locality'
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

