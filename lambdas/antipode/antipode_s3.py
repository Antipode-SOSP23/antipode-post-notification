import os
import boto3
import botocore

def _bucket(role):
  role = role.upper()
  role_region = os.environ[f"{role}_REGION"]
  return os.environ[f"S3_BUCKET__{role_region.replace('-','_').upper()}__{role}"]

KEY_SEP = '###'
def _antipode_key(k,cid):
  return f"{k}{KEY_SEP}{cid}"

def write_post(k, c):
  s3_client = boto3.client('s3')
  bucket = _bucket('writer')
  # we put to reader's bucket on the return because write post has to be read from that bucket
  # this emulates a S3 bucket "cluster" where you can write by a single name
  ant_k = _antipode_key(k,c._id)
  r = s3_client.put_object(
      Bucket=bucket,
      Key=ant_k,
      Body=os.urandom(1000000),
    )
  op = (k, r['VersionId'], c._id)
  return op

def wait(operations):
  s3_client = boto3.client('s3')
  bucket = _bucket('reader')
  # read post operations
  print(operations)
  for (k,vid,cid) in operations:
    ant_k = _antipode_key(k,cid)
    while True:
      try:
        s3_client.head_object(Bucket=bucket, Key=ant_k, VersionId=vid)
        break
      except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] in ['NoSuchKey','404']:
          print(f"[RETRY] Read {ant_k}", flush=True)
          pass
        else:
          raise

def read_post(k,c):
  s3_client = boto3.client('s3')
  bucket = _bucket('reader')
  ant_k = _antipode_key(k,c._id)
  try:
    s3_client.head_object(Bucket=bucket, Key=ant_k)
    return True
  except botocore.exceptions.ClientError as e:
    if e.response['Error']['Code'] in ['NoSuchKey','404']:
      return False
    else:
      # unknnown errors raise again
      raise