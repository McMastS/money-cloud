import schedule
import time
import yfinance as yf
import datetime
import json
import ibm_boto3
from ibm_botocore.client import Config, ClientError
import asyncio
import sys
import pandas as pd
from confluent_kafka import Producer, Consumer, KafkaException

#Global Variables
COS_ENDPOINT = "https://s3.us-east.cloud-object-storage.appdomain.cloud"
COS_API_KEY_ID = "O6M_tX17yK4Y-8o4RXI6ijQ5BRisJkSJvdUCcamgBt49"
COS_AUTH_ENDPOINT = "https://iam.cloud.ibm.com/identity/token"
COS_RESOURCE_CRN = "crn:v1:bluemix:public:cloud-object-storage:global:a/c5dc1427dfaa4a80894540effca9ecdb:58e73feb-1eff-4601-a9f7-4cceb8a81244::"
cos=ibm_boto3.resource("s3",ibm_api_key_id=COS_API_KEY_ID,ibm_service_instance_id=COS_RESOURCE_CRN,ibm_auth_endpoint=COS_AUTH_ENDPOINT,config=Config(signature_version="oauth"),endpoint_url=COS_ENDPOINT)
def main():
    listen_eventMessage('Performance_Forc','Performance_Forc',False)
def push_PerformanceForecast():
    PerformanceForecastJson=PerformanceForecast_API()

    #print(markIndexJson)
    create_text_file("mc-objstore","Performance_Forcast.json",PerformanceForecastJson)
    print()
    get_bucket_contents("mc-objstore")
    print()
    get_item("mc-objstore", "Performance_Forcast.json")

def pull_PerformanceForecast():
    try:
        markIndexJson = cos.Object("mc-objstore", "Performance_Forcast.json.json").get()
        print("File Contents: {0}".format(markIndexJson["Body"].read()))

        return markIndexJson

    except ClientError as be:
        print("CLIENT ERROR: {0}\n".format(be))
    except Exception as e:
        print("Unable to retrieve file contents: {0}".format(e))

def PerformanceForecast_API():
    names = ["DJI", "MSFT", "AAPL", "INTC", "AMAZ", "CSCO", "GOOG"]
    keyValDict = {}
    for name in names:
        tickers = yf.Ticker(name);
        openvalue = tickers.history(period="5d")['Open']
        closevalue = tickers.history(period="5d")['Close']
        data = tickers.history(period="5d")
        date = (data.index[len(data.index) - 1]).to_pydatetime()
        date += datetime.timedelta(days=1)
        openchanges = 0
        closechanges = 0
        open = 0
        close = 0
        for x in range(len(openvalue.keys()) - 1):
            openchanges = openchanges + openvalue[x + 1] - openvalue[x]
            closechanges = closechanges + closevalue[x + 1] - closevalue[x]
        closechanges = closechanges / 5
        close = close + closevalue[len(closevalue.keys()) - 1] + closechanges
        openchanges = openchanges / 5
        open = open + openvalue[len(openvalue.keys()) - 1] + openchanges
        keyValDict[name] = [date, open, close]
    out_json = json.dumps(keyValDict)
    return out_json
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

def create_text_file(bucket_name, item_name, file_text):
    print("Creating new item: {0}".format(item_name))
    try:
        cos.Object(bucket_name, item_name).put(
            Body=file_text
        )
        print("Item: {0} created!".format(item_name))
    except ClientError as be:
        print("CLIENT ERROR: {0}\n".format(be))
    except Exception as e:
        print("Unable to create text file: {0}".format(e))

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

def get_item(bucket_name, item_name):
    print("Retrieving item from bucket: {0}, key: {1}".format(bucket_name, item_name))
    try:
        file = cos.Object(bucket_name, item_name).get()
        print("File Contents: {0}".format(file["Body"].read()))
    except ClientError as be:
        print("CLIENT ERROR: {0}\n".format(be))
    except Exception as e:
        print("Unable to retrieve file contents: {0}".format(e))


def listen_eventMessage(topic_name, service_name, producer):
    driver = EventStreamsDriver(topic_name, service_name, producer)
    driver.run_task()


# code creates producertask
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


# code creates consumertask
class ConsumerTask(object):

    def __init__(self, conf, topic_name):
        self.consumer = Consumer(conf)
        self.topic_name = topic_name
        self.running = True
        self._observers = []

    def stop(self):
        self.running = False

    def print_assignment(self, consumer, partition):
        print('Assignment: ', partition)

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
                    print(msg.value())
                    self.notify_observers(msg.topic())

                    # could add something here that will tell the widget / UI to go to Object Storage
        except KeyboardInterrupt:
            sys.stderr.write("%% Aborted by user\n")
        finally:
            self.consumer.unsubscribe()
            self.consumer.close()


# code creates an instance of eventstreamsdriver
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
            'log.connection.close': False
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
            observer = Observer(self.consumer)
            self.consumer.run()


# in conjunction with the code in consumertask and eventstreamsdriver, Observer allows 'notify' upon reciept of a message and triggers our action
class Observer(object):
    def __init__(self, ConsumerTask):
        ConsumerTask.register_observer(self)

    def notify(self, ConsumerTask, *args, **kwargs):
        print('Got', args, kwargs, 'From', ConsumerTask)
        if (args == 'Performance_Forc'):
            push_PerformanceForecast()
            pull_PerformanceForecast()

main()