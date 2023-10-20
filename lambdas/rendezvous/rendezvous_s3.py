import os
import boto3
import botocore

S3_RENDEZVOUS_PATH = os.environ['S3_RENDEZVOUS_PATH']

def _bucket_key_rendezvous(bid):
  return f"{S3_RENDEZVOUS_PATH}/{bid}"

def _bucket(role):
  role = role.upper()
  role_region = os.environ[f"{role}_REGION"]
  return os.environ[f"S3_BUCKET__{role_region.replace('-','_').upper()}__{role}"]

def write_post(k, m):
  s3_client = boto3.client('s3')
  bucket = _bucket('writer')
  s3_client.put_object(
    Bucket=bucket,
    Key=k,
    Body=os.urandom(1000000)
  )
  
  s3_client.put_object(
    Bucket=_bucket('writer'),
    Key=_bucket_key_rendezvous(m),
    # body references object key for later lookup
    Body=str(k)
  )

def read_post(k):
  s3_client = boto3.client('s3')
  bucket = _bucket('reader')
  try:
    s3_client.head_object(Bucket=bucket, Key=k)
    return True
  except botocore.exceptions.ClientError as e:
    if e.response['Error']['Code'] in ['NoSuchKey','404']:
      return False
    else:
      # unknnown errors raise again
      raise