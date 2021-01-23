#!/usr/bin/env python
#
# Copyright 2020 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# =============================================================================
#
# Consume messages from Confluent Cloud
# Using Confluent Python Client for Apache Kafka
#
# =============================================================================

from confluent_kafka import Consumer,TopicPartition
import json
import ccloud_lib


if __name__ == '__main__':

    # Read arguments and configurations and initialize
    args = ccloud_lib.parse_args()
    config_file = args.config_file
    topic = args.topic
    conf = ccloud_lib.read_ccloud_config(config_file)

    # Create Consumer instance
    # 'auto.offset.reset=earliest' to start reading from the beginning of the
    #   topic if no committed offsets exist
    consumer = Consumer({
        'bootstrap.servers': conf['bootstrap.servers'],
        'sasl.mechanisms': conf['sasl.mechanisms'],
        'security.protocol': conf['security.protocol'],
        'sasl.username': conf['sasl.username'],
        'sasl.password': conf['sasl.password'],
        'group.id': 'python_example_group_1',
        'auto.offset.reset': 'earliest',
       # 'isolation.level':'read_committed',
       # 'transactional.id':'1',
    })
    #H:Consumer Groups
    '''
    consumer2 = Consumer({
        'bootstrap.servers': conf['bootstrap.servers'],
        'sasl.mechanisms': conf['sasl.mechanisms'],
        'security.protocol': conf['security.protocol'],
        'sasl.username': conf['sasl.username'],
        'sasl.password': conf['sasl.password'],
        'group.id': 'python_example_group_2',
        'auto.offset.reset': 'earliest',
    })
    consumer3 = Consumer({
        'bootstrap.servers': conf['bootstrap.servers'],
        'sasl.mechanisms': conf['sasl.mechanisms'],
        'security.protocol': conf['security.protocol'],
        'sasl.username': conf['sasl.username'],
        'sasl.password': conf['sasl.password'],
        'group.id': 'python_example_group_1',
        'auto.offset.reset': 'earliest',
    })'''
    # Subscribe to topic
   #F:varify Keys:
    consumer.assign([TopicPartition(topic,5)])
   #consumer.subscribe([topic])
   #consumer2.subscribe([topic])
   #consumer3.subscribe([topic])
   #Process messages
    total_count = 0
    
    try:
       #Read and discards all records in a topic 
       while True:
        for i in range (300):
            msg = consumer.poll(1.0)
            if msg is None:
                # No message available within timeout.
                # Initial message consumption may take up to
                # `session.timeout.ms` for the consumer group to
                # rebalance and start consuming
                print("Waiting for message or event/error in poll()")
                continue
            elif msg.error() :
                print('error: {}'.format(msg.error()))
            else:
                # Check for Kafka message
                record_key = msg.key()
                record_value = msg.value()
                record_offset=msg.offset()
                total_count += 1
                print("Consumer1:Consumed record with key {} and value {}, \
                      and updated total count to {},\
                      the offset is {}"
                      .format(record_key, record_value, total_count,record_offset))
       '''H part
        for i in range(300):
            msg2=consumer2.poll(1.0)
            if msg2 is None:
                print("Wating for message")
                continue
            elif msg2.error():
                print('error:{}'.format(msg2.error()))
            else:
                record_key2=msg2.key()
                record_value2=msg2.value()
                total_count+=1
                print("Consumer2:Consumed record with key {} and value {},\
                      and updated total count to {}"
                      .format(record_key2,record_value2,total_count))
        for i in range(300):
             msg3=consumer3.poll(1.0)
             if msg3 is None:
                 print("Wating for message:")
                 continue
             elif msg3.error():
                 print('error:{}'.format(msg3.error()))
             else:
                 record_key3=msg3.key()
                 record_value3=msg3.value()
                 total_count+=1
                 print("Consumer3:Consumed record with key {} and value {},\
                       and updated total count to {}"
                       .format(record_key3,record_value3,total_count))'''
    except KeyboardInterrupt:
        pass
    finally:
        # Leave group and commit final offsets
        consumer.close()
        #consumer2.close()
        #consumer3.close()
