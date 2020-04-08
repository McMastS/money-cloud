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
from event_streams_access import ProducerTask, EventStreamsDriver, ConsumerTask

# Global Variables
COS_ENDPOINT = "https://s3.us-east.cloud-object-storage.appdomain.cloud"
COS_API_KEY_ID = "O6M_tX17yK4Y-8o4RXI6ijQ5BRisJkSJvdUCcamgBt49"
COS_AUTH_ENDPOINT = "https://iam.cloud.ibm.com/identity/token"
COS_RESOURCE_CRN = "crn:v1:bluemix:public:cloud-object-storage:global:a/c5dc1427dfaa4a80894540effca9ecdb:58e73feb-1eff-4601-a9f7-4cceb8a81244::"
cos = ibm_boto3.resource("s3", ibm_api_key_id=COS_API_KEY_ID, ibm_service_instance_id=COS_RESOURCE_CRN,
                         ibm_auth_endpoint=COS_AUTH_ENDPOINT, config=Config(signature_version="oauth"),
                         endpoint_url=COS_ENDPOINT)


def main():
    push_PerformanceForecast()

    while True:
        schedule.run_pending()
        time.sleep(7200)
        push_PerformanceForecast()


def push_PerformanceForecast():
    PerformanceForecastJson = PerformanceForecast_API()

    # print(markIndexJson)
    create_text_file("mc-objstore", "Performance_Forcast.json", PerformanceForecastJson)
    #print()
    #get_bucket_contents("mc-objstore")
    #print()
    #get_item("mc-objstore", "Performance_Forcast.json")
    push_eventMessage()

def pull_PerformanceForecast():
    try:
        markIndexJson = cos.Object("mc-objstore", "Performance_Forcast.json").get()
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
        keyValDict[name] = [round(open,2), round(close,2)]
    out_json = json.dumps(keyValDict)
    return out_json


# get_buckets() function
# retrieves a list of available buckets in object storage
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


def push_eventMessage():
    driver = EventStreamsDriver('Performance-forcast', 'Performance-forcast', True)
    driver.run_task()



main()
