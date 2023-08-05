import os
import boto3
import botocore

def _bucket(role):
  role = role.upper()
  role_region = os.environ[f"{role}_REGION"]
  return os.environ[f"S3_BUCKET__{role_region.replace('-','_').upper()}__{role}"]

def write_post(k, c):
  s3_client = boto3.client('s3')
  bucket = _bucket('writer')
  # we put to reader's bucket on the return because write post has to be read from that bucket
  # this emulates a S3 bucket "cluster" where you can write by a single name
  r = s3_client.put_object(
      Bucket=bucket,
      Key=k,
      Body=os.urandom(1000000),
      Metadata={
        'cid': c._id,
      })
  wid = (k, r['VersionId'])
  return wid

def wait(cid, operations):
  s3_client = boto3.client('s3')
  bucket = _bucket('reader')
  # read post operations
  for (k,vid) in operations:
    while True:
      try:
        r = s3_client.head_object(Bucket=bucket, Key=k, VersionId=vid)
        if 'cid' in r['Metadata'] and r['Metadata']['cid'] == cid:
          break
      except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] in ['NoSuchKey','404']:
          print(f"[RETRY] Read {k}", flush=True)
          pass
        else:
          raise

def read_post(k,c):
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