import os
import boto3
import glob
import datetime
import psycopg2
import psycopg2.extras
import pandas as pd
import numpy as np
from io import BytesIO
from io import StringIO

BUCKET_NAME = 'search-as-service-prod'
FILE_NAME = 'bing/crawl_email_output/test/company_to_domain_cleaned_v1_test.csv'

'''--------AWS Lambda Handler---------'''
# def lambda_handler(event, context=None):

s3_client = boto3.client('s3')
resp = s3_client.get_object(Bucket=BUCKET_NAME, Key=FILE_NAME)

print("File Extracted from S3")

df_s3_data = pd.read_csv(BytesIO(resp['Body'].read()))

csv_buf = StringIO()
df_s3_data.to_csv(csv_buf,header=True, index=False)

'''--------For Download file from S3---------'''
# path = '/home/zec/Get_Email/Extract_data/file.csv'
# s3_client.download_file(BUCKET_NAME,FILE_NAME,path)

csv_buf.seek(0)

print("File converted in CSV")

key = 'bing/crawl_email_output/test/test.csv'

s3_client.put_object(Bucket=BUCKET_NAME, Body=csv_buf.getvalue(), Key=key)

print("File successfully uploaded in S3!!!")


