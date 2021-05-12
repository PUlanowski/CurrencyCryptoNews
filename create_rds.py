#!/usr/bin/python3
import boto3
import configparser
import logging
from botocore.exceptions import ClientError

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

def create_rds():
    config = configparser.ConfigParser()
    config.read('cfg.cfg')
    db_identifier = 'cnndb'
    rds = boto3.client(
        'rds',
        region_name=config['AWS']['REGION'],
        aws_access_key_id=config['AWS']['AWS_ID'],
        aws_secret_access_key=config['AWS']['AWS_SECRET'],
        aws_session_token=config['AWS']['AWS_TOKEN']
    )
    try:
        rds.create_db_instance(DBInstanceIdentifier=db_identifier,
                               AllocatedStorage=5,
                               DBName='cnn',
                               Engine='postgres',
                               StorageType='standard',
                               StorageEncrypted=False,
                               AutoMinorVersionUpgrade=True,
                               MultiAZ=False,
                               MasterUsername='postgres',
                               MasterUserPassword='pigpigpig7',
                               DBInstanceClass='db.t3.micro')

        print('Starting RDS instance with ID: {}'.format(db_identifier))

    except ClientError as e:
        logging.error(e)
        print('DB instance {} exists already, continuing to poll ...'.format(db_identifier))


if __name__ == '__main__':
    create_rds()