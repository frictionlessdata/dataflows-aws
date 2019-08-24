import os
import boto3
import logging
import mimetypes
# TODO: we use non-public API of dataflows
from dataflows.processors.dumpers.file_dumper import FileDumper
from .. import config
log = logging.getLogger(__name__)


# Module API

class S3Dumper(FileDumper):

    def __init__(self, bucket, acl,
            path='', content_type=None,
            add_filehash_to_path=False, endpoint_url=None,
            **options):

        # Process options
        super(S3Dumper, self).__init__(options)
        self._bucket = bucket
        self._acl = acl
        self._base_path = path
        self._content_type = content_type
        self._endpoint_url = endpoint_url or config.S3_ENDPOINT_URL
        self._add_filehash_to_path = add_filehash_to_path

        # Create client
        self._s3_client = boto3.client('s3', endpoint_url=self._endpoint_url)

    def process_datapackage(self, datapackage):
        super(S3Dumper, self).process_datapackage(datapackage)
        self._descirptor = datapackage.descriptor
        return datapackage

    def write_file_to_output(self, filename, path, allow_create_bucket=True):

        # Prepare content_type and key
        # We get some paths as `./data.csv`
        path = os.path.normpath(path)
        key = _generate_key(path, self._base_path, self._descirptor)
        content_type, _ = self._content_type or mimetypes.guess_type(key) or 'text/plain'

        try:

            # Skip uploading
            objs = self._s3_client.list_objects_v2(Bucket=self._bucket, Prefix=key)
            if (not path.endswith('datapackage.json')) and \
                    objs.get('KeyCount') and \
                    self._add_filehash_to_path:
                log.warning('Skipping upload of file %s as it already exists', path)
                return

            # Upload file to S3
            self._s3_client.put_object(
                ACL=self._acl,
                Body=open(filename, 'rb'),
                Bucket=self._bucket,
                ContentType=content_type,
                Key=key)

            # Calculate URL and return
            return os.path.join(self._endpoint_url, self._bucket, key)

        except self._s3_client.exceptions.NoSuchBucket as exception:

            # If you provided a custom endpoint url, we assume you are
            # using an s3 compatible server, in this case, creating a
            # bucket should be cheap and easy, so we can do it here
            if self._endpoint_url != config.S3_ENDPOINT_URL_DEFAULT and allow_create_bucket:
                self._s3_client.create_bucket(Bucket=self._bucket)
                return self.write_file_to_output(
                    filename, path, allow_create_bucket=False)

            # Otherwise just raise
            raise exception


# Internal

def _generate_key(file_path, base_path='', descriptor={}):
    try:
        format_params = {'version': 'latest'}
        format_params.update(descriptor)
        base_path = base_path.format(**format_params)
        return os.path.join(base_path, file_path)
    except KeyError:
        log.exception('datapackage.json is missing a property')
        raise
