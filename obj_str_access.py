import ibm_boto3
from ibm_botocore.client import Config, ClientError

COS_ENDPOINT = "https://s3.us-east.cloud-object-storage.appdomain.cloud"
COS_API_KEY_ID = "O6M_tX17yK4Y-8o4RXI6ijQ5BRisJkSJvdUCcamgBt49"
COS_AUTH_ENDPOINT = "https://iam.cloud.ibm.com/identity/token"
COS_RESOURCE_CRN = "crn:v1:bluemix:public:cloud-object-storage:global:a/c5dc1427dfaa4a80894540effca9ecdb:58e73feb-1eff-4601-a9f7-4cceb8a81244::"

cos = ibm_boto3.resource("s3",
    ibm_api_key_id=COS_API_KEY_ID,
    ibm_service_instance_id=COS_RESOURCE_CRN,
    ibm_auth_endpoint=COS_AUTH_ENDPOINT,
    config=Config(signature_version="oauth"),
    endpoint_url=COS_ENDPOINT
)

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

def get_item(bucket_name, item_name):
    print("Retrieving item from bucket: {0}, key: {1}".format(bucket_name, item_name))
    try:
        file = cos.Object(bucket_name, item_name).get()
        # print("File Contents: {0}".format(file["Body"].read()))
        return file['Body'].read()
    except ClientError as be:
        print("CLIENT ERROR: {0}\n".format(be))
    except Exception as e:
        print("Unable to retrieve file contents: {0}".format(e))