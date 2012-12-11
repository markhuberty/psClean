import pandas
import urllib2
import json
import boto ## for ec2 handling
import time
from pprint import pprint

def geocode_address(address, root_url):
    param = '+'.join(address.split(' '))
    this_url = root_url + param
    print this_url

    try:
        response = urllib2.urlopen(this_url)
    except urllib2.HTTPError:
        response = None

    if response:
        address = json.load(response)

        if address['status'] != 'ZERO_RESULTS':
            lat = address['results'][0]['geometry']['location']['lat']
            lng = address['results'][0]['geometry']['location']['lng']
            return (lat, lng)
        else:
            return None
    else:
        return None   

## Generate the ec2 instance
access_id = ''
access_key = ''
ec2 = boto.connect_ec2(aws_access_key_id=access_id,
                       aws_secret_access_key=access_key
                       )

reservation = ec2.run_instances(image_id='ami-15449d7c',
                                key_name='huberty_ec2_key',
                                instance_type='t1.micro',
                                security_groups=['dstk']
                                )
## Get console output:
reservations = ec2.get_all_instances()
instances = [i for r in reservations for i in r.instances]
this_instance = instances[-1]

system_time = 0
while system_time < 180:
    output = this_instance.get_console_output()
    print output.output
    time.sleep(30)
    system_time += 30

## Get the ec2 public dns
dns_name = reservation.instances[0].public_dns_name
base_url = "http://" + dns_name + "/maps/api/geocode/json?sensor=false&address="
    

## Walk across the files and geocode non-blank addresses
datadir = ''
country_files = os.listdir(datadir) ## fix this
country_files = [f for f in country_files if 'tsv' in f and ' ' not in f]

for f in country_files:
    df = pd.read_csv(f)

    counter = 0
    start_time = time.time()
    for addr in df['person_address']:
        latlng = geocode_address(addr, base_url)
        counter += 1
        if counter > 0 and counter %1000 == 0:
            this_time = time.time()
            print 'Average query time:'
            txn_average =  (this_time - start_time) / counter
            print str(txn_average)
            
    df['lat'], df['lng'] = it.izip(*latlng)
    df.to_csv('')
 
ec2.terminate_instances(instance_ids=[r.id])
