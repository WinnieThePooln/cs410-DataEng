#!/usr/bin/env python

# =============================================================================
#
# Produce messages to Confluent Cloud
# Using Confluent Python Client for Apache Kafka
#
# =============================================================================
import requests
import confluent_kafka
from confluent_kafka import Producer, KafkaError
import ccloud_lib
import json
import random
from time import sleep
if __name__ == '__main__':

    # Read arguments and configurations and initialize
    args = ccloud_lib.parse_args()
    config_file = args.config_file
    topic = args.topic
    conf = ccloud_lib.read_ccloud_config(config_file)
    
    # Create Producer instance
    producer = Producer({
        'bootstrap.servers': conf['bootstrap.servers'],
        'sasl.mechanisms': conf['sasl.mechanisms'],
        'security.protocol': conf['security.protocol'],
        'sasl.username': conf['sasl.username'],
        'sasl.password': conf['sasl.password'],
    })
    #D:Multiple Producers
    '''
    producer_2 = Producer({
        'bootstrap.servers': conf['bootstrap.servers'],
        'sasl.mechanisms': conf['sasl.mechanisms'],
        'security.protocol': conf['security.protocol'],
        'sasl.username': conf['sasl.username'],
        'sasl.password': conf['sasl.password'],
    })'''
    # Create topic if needed
    ccloud_lib.create_topic(conf, topic)

    delivered_records = 0
    # Read data from BreadCrumData
    path="/home/xiaoran/bcsample.json"
    json_file=open(path)
    json_object=json.load(json_file)
    #print(type(json_object))
    # Optional per-message on_delivery handler (triggered by poll() or flush())
    # when a message has been successfully delivered or
    # permanently failed delivery (after retries).
    def acked(err, msg):
        global delivered_records
        """Delivery report handler called on
        successful or failed delivery of message
        """
        if err is not None:
            print("Failed to deliver message: {}".format(err))
        else:
            delivered_records += 1
            print("Produced record to topic {} partition [{}] @ offset {}"
                  .format(msg.topic(), msg.partition(), msg.offset()))
    for n in range(1000):
       # F:Varying Keys
        record_key =str(random.randint(1,5))
        '''I part
        if n==0:
             producer.init_transactions();
        '''
        #record_key="alice"
        record_value = json.dumps(json_object[n])
        print("Producing record: {}\t{}".format(record_key, record_value))
        producer.produce(topic, key=record_key, value=record_value, on_delivery=acked,partition=int(record_key))
        '''I part:
        if (n+1)==4:
            sleep(2)
            p=random.randint(1,10)
            if(p>=5):
                try:
                    producer.commit_transaction()
                    print("Commit transaction sucess!")
                except confluent_kafka.KafkaException as e:
                    print(e)
            else:
                try:
                    producer.abort_transaction()
                    print("Commit transaction fail!")
                except confluent_kafka.KafkaException as e:
                    print(e)
        '''
        #producer.flush()
        #E:sleep 250 msec
        #sleep(0.25)
        # p.poll() serves delivery reports (on_delivery)
        # from previous produce() calls.
        #if (n+1)%5==0:
        #   sleep(2)
        #if (n+1)%15==0:
        #    producer.flush()
        #producer.poll(0)
    producer.flush()

    print("{} messages were produced to topic {}!".format(delivered_records, topic))
