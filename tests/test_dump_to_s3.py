import os
import json
import boto3
import pytest
import random
import string
import requests
from moto import mock_s3
from dataflows import Flow, load
from dataflows_aws import dump_to_s3


# Setup

os.environ['S3_ENDPOINT_URL'] = 'http://localhost:5000'


# Tests

def test_dump_to_s3(s3_client, bucket):

    # Dump to S3 using the processor
    flow = Flow(
        load('data/data.csv'),
        dump_to_s3(
            bucket=bucket,
            acl='private',
            path='my/datapackage',
            endpoint_url=os.environ['S3_ENDPOINT_URL'],
        ),
    )
    flow.process()

    # Check datapackage.json content
    response = s3_client.get_object(Bucket=bucket, Key='my/datapackage/datapackage.json')
    descriptor = json.loads(response['Body'].read().decode('utf-8'))
    assert descriptor['resources'][0]['schema']['fields'][0]['name'] == 'id'
    assert descriptor['resources'][0]['schema']['fields'][1]['name'] == 'name'

    # Check data.csv content
    response = s3_client.get_object(Bucket=bucket, Key='my/datapackage/data.csv')
    contents = response['Body'].read().decode('utf-8')
    assert contents == 'id,name\r\n1,english\r\n2,中国人\r\n'


def test_dump_to_s3_non_existent_bucket(s3_client, bucket):

    # Delete bucket
    s3_client.delete_bucket(Bucket=bucket)

    # Dump to S3 using the processor
    flow = Flow(
        load('data/data.csv'),
        dump_to_s3(
            bucket=bucket,
            acl='private',
            path='my/datapackage',
            endpoint_url=os.environ['S3_ENDPOINT_URL'],
        ),
    )
    flow.process()

    # Check datapackage.json content
    response = s3_client.get_object(Bucket=bucket, Key='my/datapackage/datapackage.json')
    descriptor = json.loads(response['Body'].read().decode('utf-8'))
    assert descriptor['resources'][0]['schema']['fields'][0]['name'] == 'id'
    assert descriptor['resources'][0]['schema']['fields'][1]['name'] == 'name'

    # Check data.csv content
    response = s3_client.get_object(Bucket=bucket, Key='my/datapackage/data.csv')
    contents = response['Body'].read().decode('utf-8')
    assert contents == 'id,name\r\n1,english\r\n2,中国人\r\n'


# Fixtures

@pytest.fixture
def s3_client():
    s3_client = boto3.client('s3', endpoint_url=os.environ.get('S3_ENDPOINT_URL'))
    return s3_client


@pytest.fixture
def bucket(s3_client):
    bucket = 'bucket_%s' % ''.join(random.choice(string.digits) for _ in range(16))
    s3_client.create_bucket(Bucket=bucket, ACL='public-read')
    return bucket
