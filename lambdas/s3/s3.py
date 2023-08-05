import boto3
import botocore
import os

def _bucket(role):
  role = role.upper()
  role_region = os.environ[f"{role}_REGION"]
  return os.environ[f"S3_BUCKET__{role_region.replace('-','_').upper()}__{role}"]

def write_post(k):
  s3_client = boto3.client('s3')
  # we put to reader's bucket on the return because write post has to be read from that bucket
  # this emulates a S3 bucket "cluster" where you can write by a single name
  r = s3_client.put_object(
      Bucket=_bucket('writer'),
      Key=str(k),
      Body=os.urandom(1000000),
    )
  op = (k, r['VersionId'])
  return op

def read_post(k):
  s3_client = boto3.client('s3')
  try:
    s3_client.head_object(Bucket=_bucket('reader'), Key=str(k))
    return True
  except botocore.exceptions.ClientError as e:
    if e.response['Error']['Code'] in ['NoSuchKey','404']:
      return False
    else:
      # unknnown errors raise again
      raise

def clean():
  None

def stats():
  return {}