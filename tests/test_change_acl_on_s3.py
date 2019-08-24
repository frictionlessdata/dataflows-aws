import os
import boto3
import pytest
import random
import string
import requests
from moto import mock_s3
from dataflows import Flow, load
from dataflows_aws import change_acl_on_s3


# Setup

os.environ['S3_ENDPOINT_URL'] = 'http://localhost:5000'


# Tests

def test_change_acl_on_s3(s3_client, bucket):

    # Prepare paths
    paths = [
        'my/private/datasets/README.md',
        'my/private/datasets/datapackage.json',
        'my/private/datasets/data/mydata.csv',
        'my/public/datasets/data/mydata.csv',
    ]

    # Fill the S3 bucket
    for path in paths:
        s3_client.put_object(Body='body', Bucket=bucket, Key=path, ACL='public-read')

    # Assert all contents are public by default
    for path in paths:
        url = '{}/{}/{}'.format(os.environ['S3_ENDPOINT_URL'], bucket, path)
        assert requests.get(url).status_code == 200

    # Set private ACL using the processor
    flow = Flow(
        load('data/data.csv'),
        change_acl_on_s3(
            bucket=bucket,
            acl='private',
            path='my/private/datasets',
            endpoint_url=os.environ['S3_ENDPOINT_URL'],
        ),
    )
    flow.process()

    # Assert only public contents are public
    for path in paths:
        url = '{}/{}/{}'.format(os.environ['S3_ENDPOINT_URL'], bucket, path)
        assert requests.get(url).status_code == (200 if 'public' in path else 403)


def test_change_acl_on_s3_handles_non_existing_keys(s3_client, bucket):

    # Set private ACL using the processor
    # Assert not failing (does nothing)
    flow = Flow(
        load('data/data.csv'),
        change_acl_on_s3(
            bucket=bucket,
            acl='private',
            path='my/non-existing/datasets',
            endpoint_url=os.environ['S3_ENDPOINT_URL'],
        ),
    )
    flow.process()


def test_change_acl_on_s3_no_path_provided(s3_client, bucket):

    # Prepare paths
    paths = [
        'my/private/datasets/file_1.csv'
        'my/private/datasets/file_2.csv'
    ]

    # Fill the S3 bucket
    for path in paths:
        s3_client.put_object(Body='body', Bucket=bucket, Key=path, ACL='public-read')

    # Set private ACL using the processor
    flow = Flow(
        load('data/data.csv'),
        change_acl_on_s3(
            bucket=bucket,
            acl='private',
            endpoint_url=os.environ['S3_ENDPOINT_URL'],
        ),
    )
    flow.process()

    # Assert everything is private now
    for path in paths:
        url = '{}/{}/{}'.format(os.environ['S3_ENDPOINT_URL'], bucket, path)
        assert requests.get(url).status_code == 403


@pytest.mark.skipif(not os.environ.get('CI'), reason='CI')
def test_change_acl_on_s3_handles_more_than_1000_files(s3_client, bucket):

    # Prepare paths
    paths = []
    for index in range(1, 1101):
        path = 'my/private/datasets/file_%s.csv' % index
        paths.append(path)

    # Fill the S3 bucket
    for path in paths:
        s3_client.put_object(Body='body', Bucket=bucket, Key=path, ACL='public-read')

    # Set private ACL using the processor
    flow = Flow(
        load('data/data.csv'),
        change_acl_on_s3(
            bucket=bucket,
            acl='private',
            path='my/private/datasets',
            endpoint_url=os.environ['S3_ENDPOINT_URL'],
        ),
    )
    flow.process()

    # Assert everything is private now
    for path in paths:
        url = '{}/{}/{}'.format(os.environ['S3_ENDPOINT_URL'], bucket, path)
        assert requests.get(url).status_code == 403


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
