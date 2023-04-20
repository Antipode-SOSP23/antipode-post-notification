import os
import boto3
import botocore
import json
from rendezvous_shim import RendezvousShim

S3_RENDEZVOUS_PATH = os.environ['S3_RENDEZVOUS_PATH']

class RendezvousS3(RendezvousShim):
  def __init__(self, conn, region):
    super().__init__(region)
    self.bucket = conn
    self.s3_client = boto3.client('s3')
    self.continuation_token = None

  def _bucket_key_rendezvous(self, rid):
    return f"{S3_RENDEZVOUS_PATH}/{rid}"
  
  def _bucket_prefix_rendezvous(self):
    return f"{S3_RENDEZVOUS_PATH}/"
  
  def _find_object(self, rid, obj_key, metadata_created_at):
    try:
      response = self.s3_client.head_object(Bucket=self.bucket, Key=obj_key)

      # found the object version we were looking for with the correct rid
      if response.get('Metadata') and response['Metadata'].get('rendezvous') == rid:
        return True
      
      # current object corresponds to a newer version
      if response['LastModified'] >= metadata_created_at:
        return True
      
    except botocore.exceptions.ClientError as e:
      if e.response['Error']['Code'] in ['NoSuchKey','404']:
        pass
      else:
        raise
    
    return False

# ----------------
# Current request
# ----------------

  def read_metadata(self, rid):
    while True:
      try:
        response = self.s3_client.get_object(Bucket=self.bucket, Key=self._bucket_key_rendezvous(rid))
        obj = json.loads(response['Body'].read())

        # wait until object (post) is available
        while not self._find_object(obj['rid'], obj['obj_key'], response['LastModified']):
          self.inconsistency = True

        return obj['bid']

      except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] in ['NoSuchKey','404']:
          self.inconsistency = True
          pass
        else:
          raise

# -------------
# All requests
# -------------

  def _parse_metadata(self, item):
    return item['rid'], item['bid']

  def read_all_metadata(self):
    if not self.continuation_token:
      response = self.s3_client.list_objects_v2(Bucket=self.bucket, Prefix=self._bucket_prefix_rendezvous())
    else:
      response = self.s3_client.list_objects_v2(Bucket=self.bucket, Prefix=self._bucket_prefix_rendezvous(), ContinuationToken=self.continuation_token)

    # track continuation token to continue reading in the next function call
    self.continuation_token = response.get('NextContinuationToken', None)

    objects = []
    for obj in response.get('Contents', []):
      response = self.s3_client.get_object(Bucket=self.bucket, Key=obj['Key'])
      metadata = json.loads(response['Body'].read())

      # check if object (post) is available
      if metadata['rid'] not in self.metadata and self._find_object(metadata['rid'], metadata['obj_key'], obj['LastModified']):
        objects.append(metadata)
    
    return objects
