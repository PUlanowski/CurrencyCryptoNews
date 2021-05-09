#!/usr/bin/python3
import boto3
import configparser
import logging
from botocore.exceptions import ClientError

logging.getLogger().setLevel(logging.INFO)

bucket_name = 'cnn-storage'

def s3_client_start():
    config = configparser.ConfigParser()
    config.read('aws.cfg')
    s3_client = boto3.client(
        's3',
        aws_access_key_id=config['AWS']['AWS_ID'],
        aws_secret_access_key=config['AWS']['AWS_SECRET'],
        aws_session_token=config['AWS']['AWS_TOKEN'],
        region_name=config['AWS']['REGION']
    )
    return s3_client

def create_bucket(bucket_name, region=None):
    s3_client = s3_client_start()
    try:
        bucket_check = s3_client.list_buckets()
        buckets = [bucket['Name'] for bucket in bucket_check['Buckets']]

        if (bucket_name in buckets):
            logging.warning('Bucket {} already exists'.format(bucket_name))
        else:
            s3_client.create_bucket(Bucket = bucket_name)
            logging.info('Bucket {} created'.format(bucket_name))

    except ClientError as e:
        logging.error(e)
        return False
    return True

if __name__=="__main__":
    s3_client_start()
    create_bucket(bucket_name, region=None)