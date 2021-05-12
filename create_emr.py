#!/usr/bin/python3
import boto3
import configparser
import logging
from botocore.exceptions import ClientError

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()


def emr_client_start():
    config = configparser.ConfigParser()
    config.read('cfg.cfg')
    global emr_client
    emr_client = boto3.client(
        'emr',
        region_name=config['AWS']['REGION'],
        aws_access_key_id=config['AWS']['AWS_ID'],
        aws_secret_access_key=config['AWS']['AWS_SECRET'],
        aws_session_token=config['AWS']['AWS_TOKEN']
    )
    return emr_client


def create_emr(emr_client):
    try:
        emrcluster = emr_client.run_job_flow(
            Name='CurrencyCryptoNews',
            ReleaseLabel='emr-5.33.0',
            Instances={
                'InstanceGroups': [
                    {
                        'Name': "Master nodes",
                        'Market': 'ON_DEMAND',
                        'InstanceRole': 'MASTER',
                        'InstanceType': 'm4.large',
                        'InstanceCount': 1,
                    },
                    {
                        'Name': "Slave nodes",
                        'Market': 'ON_DEMAND',
                        'InstanceRole': 'CORE',
                        'InstanceType': 'm4.large',
                        'InstanceCount': 1,
                    }
                ],
                'Ec2KeyName': 'hadoop',
                'KeepJobFlowAliveWhenNoSteps': True,
                'TerminationProtected': False,
                'Ec2SubnetId': 'subnet-10e4cf1e',
            },
            Applications=[
                {'Name': 'Spark'},
                {'Name': 'Hive'},
                {'Name': 'Hue'},
                {'Name': 'HBase'},
                {'Name': 'HCatalog'}
            ],

            VisibleToAllUsers=True,
            JobFlowRole='EMR_EC2_DefaultRole',
            ServiceRole='EMR_DefaultRole',
        )
        global cluster_id
        cluster_id = emrcluster['JobFlowId']
        if  (emrcluster['JobFlowId'] != ''):
            logging.info('EMR creation request sent to AWS cluster')
            print(
                'ClusterID: {} , DateCreated: {} , RequestId: {}'
                .format(
                    emrcluster['JobFlowId'],
                    emrcluster['ResponseMetadata']['HTTPHeaders']['date'],
                    emrcluster['ResponseMetadata']['RequestId']
                    )
            )
        else:
            logging.info('EMR not created')

    except ClientError as e:
        logging.error(e)
        return False
    return cluster_id


def terminate_cluster(emr_client, cluster_id):

    try:
        emr_client.terminate_job_flows(JobFlowIds=[cluster_id])
        logger.info("Terminated cluster %s.", cluster_id)
    except ClientError:
        logger.exception("Couldn't terminate cluster %s.", cluster_id)
        raise

if __name__=="__main__":
    emr_client_start()
    create_emr(emr_client)
    #terminate_cluster(emr_client, cluster_id)