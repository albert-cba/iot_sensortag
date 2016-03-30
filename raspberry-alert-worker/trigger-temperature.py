# Copyright 2013-2014 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"). You may not
# use this file except in compliance with the License. A copy of the License
# is located at
#
#     http://aws.amazon.com/apache2.0/
#
# or in the "LICENSE.txt" file accompanying this file. This file is
# distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the specific language
# governing permissions and limitations under the License.

from __future__ import print_function

import sys
import re
import boto
import argparse
import json
import threading
import time
import datetime

from argparse import RawTextHelpFormatter
from boto.kinesis.exceptions import ProvisionedThroughputExceededException
import RPi.GPIO as GPIO

# To preclude inclusion of aws keys into this code, you may temporarily add
# your AWS credentials to the file:
#     ~/.boto
# as follows:
#     [Credentials]
#     aws_access_key_id = <your access key>
#     aws_secret_access_key = <your secret key>


kinesis = boto.kinesis.connect_to_region("ap-northeast-1")
iter_type_at = 'AT_SEQUENCE_NUMBER'
iter_type_after = 'AFTER_SEQUENCE_NUMBER'
iter_type_trim = 'TRIM_HORIZON'
iter_type_latest = 'LATEST'
 
def process_to_generate_light(records):
    for record in records:
        jsonstring = record['Data'].lower()
        json_data = json.loads(jsonstring)
        print (json_data)


class KinesisWorker(threading.Thread):
    """The Worker thread that repeatedly gets records from a given Kinesis
    stream."""
    def __init__(self, stream_name, shard_id, iterator_type, name=None, group=None, args=(), kwargs={}):
        super(KinesisWorker, self).__init__(name=name, group=group,
                                          args=args, kwargs=kwargs)
        self.stream_name = stream_name
        self.shard_id = str(shard_id)
        self.iterator_type = iterator_type
        self.total_records = 0

    def run(self):
        my_name = threading.current_thread().name
        print ('+ LightWorker:', my_name)
        print ('+-> working with iterator:', self.iterator_type)
        response = kinesis.get_shard_iterator(self.stream_name,
            self.shard_id, self.iterator_type)
        next_iterator = response['ShardIterator']
        print ('+-> getting next records using iterator:', next_iterator)

        counter = 0
        isLightOn = False
        while True:
            try:
                response = kinesis.get_records(next_iterator, limit=25)
                self.total_records += len(response['Records'])
                counter = counter + 1
                print ("Counter:", str(counter) + " and " + str(isLightOn))

                if len(response['Records']) > 0:
                    print ('\n+-> {1} Got {0} Worker Records'.format(len(response['Records']), my_name))
                    process_to_generate_light(response['Records'])

                    if isLightOn is False:
                        print ("Lights up")
                        GPIO.setup(27,GPIO.OUT)
                        GPIO.output(27,GPIO.HIGH)
                        isLightOn = True

                    counter = 0
                else:
                    if counter >= 3 and isLightOn is True:
                        print ("Lights off")
                        GPIO.setup(27,GPIO.OUT)
                        GPIO.output(27,GPIO.LOW)
                        isLightOn = False

                next_iterator = response['NextShardIterator']
                time.sleep(0.5)

            except ProvisionedThroughputExceededException as ptee:
                print (ptee.message)
                time.sleep(5)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='''Create or connect to a Kinesis stream and create workers
that hunt for the word "egg" in records from each shard.''',
        formatter_class=RawTextHelpFormatter)
    parser.add_argument('stream_name',
        help='''the name of the Kinesis stream to either create or connect''')

    args = parser.parse_args()

    print ("Temperature off")
    GPIO.setmode(GPIO.BCM)
    GPIO.cleanup()
    GPIO.setwarnings(False)
    GPIO.setup(27,GPIO.OUT)
    GPIO.output(27,GPIO.LOW)

    stream = kinesis.describe_stream(args.stream_name)
    print (json.dumps(stream, sort_keys=True, indent=2, separators=(',', ': ')))
    shards = stream['StreamDescription']['Shards']
    print ('# Shard Count:', len(shards))

    threads = []
    start_time = datetime.datetime.now()
    for shard_id in xrange(len(shards)):
        worker_name = 'shard_worker:%s' % shard_id
        print ('#-> shardId:', shards[shard_id]['ShardId'])
        worker = KinesisWorker(
            stream_name=args.stream_name,
            shard_id=shards[shard_id]['ShardId'],
            # iterator_type=iter_type_trim,  # uses TRIM_HORIZON
            iterator_type=iter_type_latest,  # uses LATEST
            name=worker_name
            )
        worker.daemon = True
        threads.append(worker)
        print ('#-> starting: ', worker_name)
        worker.start()

    # Wait for all threads to complete
    for t in threads:
        t.join()
    finish_time = datetime.datetime.now()
    duration = (finish_time - start_time).total_seconds()

    print ("-=> Exiting Worker Main <=-")
    print ("     Total Time:", duration)
    print ("  Worker sleep interval:", 0.5)

    GPIO.cleanup()
    GPIO.setwarnings(False)
    GPIO.setup(27,GPIO.OUT)
    GPIO.output(27,GPIO.LOW)