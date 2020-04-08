##Currency_API.py - Da Zhong - Student ID 250856446
#This service publishes the performance of Currency indices for the previous day.
#.

#import libraries
import schedule
import time
import yfinance as yf
import datetime
import json
import ibm_boto3
from ibm_botocore.client import Config, ClientError
from event_streams_access import ProducerTask, EventStreamsDriver, ConsumerTask

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
    currDate=datetime.date.today()
    #determine yesterdays date
    yesterDate=currDate - datetime.timedelta(days=4)


    #initial call to push data to object storage (upon service startup)
    push_Currency()


    # Every day at 9:35am market Index tracker is called / updated.
    #schedule.every().day.at("09:35").do(push_Currency)
    # Every day at 4:00pm market Index tracker is called / updated.
    #schedule.every().day.at("16:00").do(push_Currency)

    while True:
        schedule.run_pending()
        time.sleep(7200)
        push_Currency()


def push_Currency():
    markIndexJson=Currency_API()

    create_text_file("mc-objstore","Curr_trkr.json",markIndexJson)
    print()
    push_eventMessage()
    print()
    get_bucket_contents("mc-objstore")
    print()

def Currency_API():
    #create dictionary
    keyValDict={}

    #pull data from API
    oneValue = yf.download('CD=F CNH=F EURUSD=X GBPUSD=X JPY=X', period='1d')

    #access and isolate opening values
    openValue=oneValue['Open']
    #access and isolate closing values
    closeValue=oneValue['Close']
    #instantiate a change variable
    change=0

    #iterate through dataframe key:values for indices
    for x in range(len(closeValue.keys())):
        #calculate the change from daily opening value to current value
        change=closeValue.values[0][x]-openValue.values[0][x]

        #for each of the indices, create a dictionary entry with closing value and daily change
        keyValDict[closeValue.keys()[x]]=[closeValue.values[0][x], change]

    #open json file, convert and write dictionary to json file
    #with open("Curr_trkr.json", "w") as outfile:

    #convert dict to a json
    out_json=json.dumps(keyValDict)
    return out_json

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
    driver = EventStreamsDriver('Currency_Tracker', 'Currency_Tracker', True)
    driver.run_task()


main() #start program
