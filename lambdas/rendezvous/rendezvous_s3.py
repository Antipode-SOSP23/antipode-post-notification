import os
import boto3
import botocore
import json
from rendezvous_shim import RendezvousShim
from datetime import datetime, timedelta

S3_RENDEZVOUS_PATH = os.environ['S3_RENDEZVOUS_PATH']

class RendezvousS3(RendezvousShim):
  def __init__(self, conn, region):
    super().__init__(region)
    self.bucket = conn
    self.s3_client = boto3.client('s3')
    self.continuation_token = None
    
  def _parse_metadata(self, item):
    return item['rid'], item['service'], item['ts']
  
  def _find_object(self, rid, object_key, metadata_created_at):
    try:
      response = self.s3_client.head_object(Bucket=self.bucket, Key=object_key)

      # found the object version we were looking for with the correct rid
      if response.get('Metadata') and response['Metadata'].get('rendezvous_rid') == rid:
        return True
      
      # current object corresponds to a newer version
      if response['LastModified'] >= metadata_created_at:
        return True
      
    except botocore.exceptions.ClientError as e:
      if e.response['Error']['Code'] in ['NoSuchKey','404']:
        print(f"[ERROR] S3 exception: object key {object_key} not found. Retrying again later...", flush=True)
      else:
        print(f"[ERROR] S3 exception finding object", flush=True)
    
    return False
  
  def _read_metadata(self):
    objects = []

    # retrieve keys in rendezvous folder
    try:
      if not self.continuation_token:
        response = self.s3_client.list_objects_v2(Bucket=self.bucket, Prefix=f'{S3_RENDEZVOUS_PATH}/')
      else:
        response = self.s3_client.list_objects_v2(Bucket=self.bucket, Prefix=f'{S3_RENDEZVOUS_PATH}/', ContinuationToken=response['NextContinuationToken'])

      # s3 can only list up to 1000 objects in a single request with no continuation token
      # track continuation token to continue listing object keys in the next function call and reduce amount of returned items
      self.continuation_token = response.get('NextContinuationToken', None)

      # retrieve content of objects
      for obj in response.get('Contents', []):
        response = self.s3_client.get_object(Bucket=self.bucket, Key=obj['Key'])
        # response body returns a streaming body so we have to read from it
        metadata = json.loads(response['Body'].read())

        # check if object (post) is available
        if self._find_object(metadata['rid'], metadata['object_key'], obj['LastModified']):
          objects.append(metadata)

    except botocore.exceptions.BotoCoreError as e:
      print(f"[ERROR] S3 exception reading rendezvous metadata: {e}", flush=True)

    return objects
