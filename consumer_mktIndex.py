##Market_Index_Tracker.py - Michael Rist - Student ID 250996815
#This service publishes the performance of key market indices for the previous day.
#.

#import libraries
import schedule
import time
import yfinance as yf
import datetime
import json
import ibm_boto3
from ibm_botocore.client import Config, ClientError
import asyncio
import sys
from confluent_kafka import Producer, Consumer, KafkaException
#from event_streams_access import ProducerTask, EventStreamsDriver, ConsumerTask

#Global Variables

#for Object storage
COS_ENDPOINT = "https://s3.us-east.cloud-object-storage.appdomain.cloud"
COS_API_KEY_ID = "O6M_tX17yK4Y-8o4RXI6ijQ5BRisJkSJvdUCcamgBt49"
COS_AUTH_ENDPOINT = "https://iam.cloud.ibm.com/identity/token"
COS_RESOURCE_CRN = "crn:v1:bluemix:public:cloud-object-storage:global:a/c5dc1427dfaa4a80894540effca9ecdb:58e73feb-1eff-4601-a9f7-4cceb8a81244::"
cos=ibm_boto3.resource("s3",ibm_api_key_id=COS_API_KEY_ID,ibm_service_instance_id=COS_RESOURCE_CRN,ibm_auth_endpoint=COS_AUTH_ENDPOINT,config=Config(signature_version="oauth"),endpoint_url=COS_ENDPOINT)


#Main function opens a socket and, using helper function, sends data prior to closing the socket connection.
def main():
    #get current date
    #currDate=datetime.date.today()

    #initial call to push data to object storage (upon service startup)
    #pull_marketIndexTracker()

    #instantiate consumer eventstreamsdriver and run
    listen_eventMessage()


#function retreives the market index data from object storage
def pull_marketIndexTracker():
    #markIndexJson=get_item("mc-objstore", "mark_inx_trkr.json")
    try:
        markIndexJson = cos.Object("mc-objstore", "mark_inx_trkr.json").get()
        print('Accessing Object Storage, retrieving: mark_inx_trkr.json')
        print()
        print("File Contents: {0}".format(markIndexJson["Body"].read()))

        return markIndexJson

    except ClientError as be:
        print("CLIENT ERROR: {0}\n".format(be))
    except Exception as e:
        print("Unable to retrieve file contents: {0}".format(e))

#get_buckets() function
#retrieves a list of available buckets in object storage
def get_buckets():
    print("Retrieving list of buckets")
    try:
        buckets = cos.buckets.all()
        for bucket in buckets:
            print("Bucket Name: {0}".format(bucket.name))
    except ClientError as be:
        print("CLIENT ERROR: {0}\n".format(be))
    except Exception as e:
        print("Unable to retrieve list buckets: {0}".format(e))

#gets contents list of files in buckets
def get_bucket_contents(bucket_name):
    print("Retrieving bucket contents from: {0}".format(bucket_name))
    try:
        files = cos.Bucket(bucket_name).objects.all()
        for file in files:
            print("Item: {0} ({1} bytes).".format(file.key, file.size))
    except ClientError as be:
        print("CLIENT ERROR: {0}\n".format(be))
    except Exception as e:
        print("Unable to retrieve bucket contents: {0}".format(e))

#gets a specific item / file from object storage bucket
def get_item(bucket_name, item_name):
    print("Retrieving item from bucket: {0}, key: {1}".format(bucket_name, item_name))
    try:
        file = cos.Object(bucket_name, item_name).get()
        print("File Contents: {0}".format(file["Body"].read()))
        return file
    except ClientError as be:
        print("CLIENT ERROR: {0}\n".format(be))
    except Exception as e:
        print("Unable to retrieve file contents: {0}".format(e))

#creates an instance of EventStreamsDriver for the consumer and
def listen_eventMessage():
    driver = EventStreamsDriver('Market-Idx','Market-Idx',False)
    driver.run_task()

#code creates producertask
class ProducerTask(object):
    def __init__(self, conf, topic_name):
        self.topic_name = topic_name
        self.producer = Producer(conf)
        self.counter = 0
        self.running = True

    def stop(self):
        self.running = False

    def on_delivery(self, err, msg):
        if err:
            print('Delivery report: Failed sending message {0}'.format(msg.value()))
            print(err)
            # We could retry sending the message
        else:
            print('Message produced, offset: {0}'.format(msg.offset()))

    def run(self):
        self.producer.produce(self.topic_name, "Hello! This is a message! 2", callback=self.on_delivery)
        self.producer.poll(0)

        self.producer.flush()

#code creates consumertask
class ConsumerTask(object):

    def __init__(self, conf, topic_name):
        self.consumer = Consumer(conf)
        self.topic_name = topic_name
        self.running = True
        self._observers = []

    def stop(self):
        self.running = False

    def print_assignment(self, consumer, partition):
        print('Assignment - subscribing to topic: ', partition)

    def register_observer(self, observer):
        self._observers.append(observer)

    def notify_observers(self, *args, **kwargs):
        for observer in self._observers:
            observer.notify(self, *args, **kwargs)

    def run(self):
        self.consumer.subscribe([self.topic_name], on_assign=self.print_assignment)

        try:
            while True:
                msg = self.consumer.poll(1)
                if msg is None:
                    continue
                if msg.error():
                    raise KafkaException(msg.error())
                else:
                    sys.stderr.write('%% %s [%d] at offset %d with key %s:\n' %
                                    (msg.topic(), msg.partition(), msg.offset(),
                                    str(msg.key())))
                    #print(msg.value())
                    self.notify_observers(msg.topic())

                    #could add something here that will tell the widget / UI to go to Object Storage
        except KeyboardInterrupt:
            sys.stderr.write("%% Aborted by user\n")
        finally:
            self.consumer.unsubscribe()
            self.consumer.close()

#code creates an instance of eventstreamsdriver
class EventStreamsDriver(object):
    def __init__(self, topic_name, service_name, producer):
        self.consumer = None
        self.producer = None

        if producer:
            self.run_producer = True
        else:
            self.run_producer = False

        self.topic_name = topic_name
        self.base_config = {
            'bootstrap.servers': 'broker-1-9tl582p7src9jz2d.kafka.svc03.us-south.eventstreams.cloud.ibm.com:9093,broker-5-9tl582p7src9jz2d.kafka.svc03.us-south.eventstreams.cloud.ibm.com:9093,broker-4-9tl582p7src9jz2d.kafka.svc03.us-south.eventstreams.cloud.ibm.com:9093,broker-2-9tl582p7src9jz2d.kafka.svc03.us-south.eventstreams.cloud.ibm.com:9093,broker-3-9tl582p7src9jz2d.kafka.svc03.us-south.eventstreams.cloud.ibm.com:9093,broker-0-9tl582p7src9jz2d.kafka.svc03.us-south.eventstreams.cloud.ibm.com:9093',
            'security.protocol': 'SASL_SSL',
            'sasl.mechanisms': 'PLAIN',
            'sasl.username': 'token',
            'sasl.password': 'nbHuMTLnLi-rmmhP22gUQSUuExarXsEpF8z49FSBLJRj',
            'api.version.request': True,
            'broker.version.fallback': '0.10.2.1',
            'log.connection.close' : False
        }
        self.prod_config = {
            'client.id': service_name + '-producer'
        }
        self.cons_config = {
            'client.id': service_name + '-consumer',
            'group.id': 'money-cloud-services'
        }

        for key in self.base_config:
            self.cons_config[key] = self.base_config[key]
            self.prod_config[key] = self.base_config[key]

    def run_task(self):
        # tasks = []
        if self.run_producer:
            self.producer = ProducerTask(self.prod_config, self.topic_name)
            self.producer.run()
            # tasks.append(asyncio.ensure_future(self.producer.run()))
        else:
            self.consumer = ConsumerTask(self.cons_config, self.topic_name)
            observer=Observer(self.consumer)
            self.consumer.run()

#in conjunction with the code in consumertask and eventstreamsdriver, Observer allows 'notify' upon reciept of a message and triggers our action
class Observer(object):
    def __init__(self, ConsumerTask):
        ConsumerTask.register_observer(self)

    def notify(self, ConsumerTask, *args, **kwargs):
        #print('Got', args, kwargs, 'From', ConsumerTask)
        if(args[0]=='Market-Idx'):
            print("Message Recieved from: ", args[0])
            print()
            pull_marketIndexTracker()
            print()
            print("-------------------------------------------------------")



main() #start program
