#!/usr/bin/python3
import boto3
import configparser
import logging
from botocore.exceptions import ClientError
from create_s3_bucket import bucket_name

config = configparser.ConfigParser()
config.read('aws.cfg')
logging.getLogger().setLevel(logging.INFO)
bucket_name = bucket_name

def s3_client_start(config):
    s3_client = boto3.client(
        's3',
        aws_access_key_id=config['AWS']['AWS_ID'],
        aws_secret_access_key=config['AWS']['AWS_SECRET'],
        aws_session_token=config['AWS']['AWS_TOKEN'],
        region_name=config['AWS']['REGION']
    )
    return s3_client

def move_resources_to_s3(config, bucket_name):
    s3_client = s3_client_start(config)
    try:
        for filename in config['KAGGLE'].values():
            s3_client.upload_file(filename, bucket_name, filename)
            logging.info('File {} moved to S3'.format(filename))

    except ClientError as e:
        logging.error(e)
        return False
    return True

if __name__=="__main__":
    s3_client_start(config)
    move_resources_to_s3(config, bucket_name)

