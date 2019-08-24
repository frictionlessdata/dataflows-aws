import os


# General

S3_ENDPOINT_URL_DEFAULT = 'https://s3.amazonaws.com'
S3_ENDPOINT_URL = os.environ.get('S3_ENDPOINT_URL', S3_ENDPOINT_URL_DEFAULT)
