import os
import boto3


# Module API

def change_acl_on_s3(bucket, acl, path):

    def func(package):

        # Prepare client
        endpoint_url = os.environ.get('S3_ENDPOINT_URL')
        s3_client = boto3.client('s3', endpoint_url=endpoint_url)

        # Change ACL
        # list_objects returns max 1000 keys (even if MaxKeys is >1000)
        marker = ''
        is_truncated = True
        while is_truncated:
            objs = s3_client.list_objects(Bucket=bucket, Prefix=path, Marker=marker)
            is_truncated = objs.get('IsTruncated')
            for obj in objs.get('Contents', []):
                s3_client.put_object_acl(Bucket=bucket, Key=obj['Key'], ACL=acl)
                marker = obj['Key']

    return func
