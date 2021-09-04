import boto3
import botocore
import os
from datetime import datetime

def _bucket(role):
  role = role.upper()
  role_region = os.environ[f"{role}_REGION"]
  return os.environ[f"S3_BUCKET__{role_region.replace('-','_').upper()}__{role}"]

def write_post(i,k):
  s3_client = boto3.client('s3')
  op = (_bucket('writer'), str(k))
  s3_client.put_object(
    Bucket=op[0],
    Key=op[1],
    Body=os.urandom(1000000),
  )
  return op

def read_post(k, evaluation):
  s3_client = boto3.client('s3')
  # evaluation keys to fill
  # {
  #   'read_post_retries' : 0,
  #   'ts_read_post_spent_ms': None,
  # }

  # read key of post
  ts_read_post_start = datetime.utcnow().timestamp()
  while True:
    try:
      s3_client.head_object(Bucket=_bucket('reader'), Key=str(k))
      evaluation['ts_read_post_spent_ms'] = int((datetime.utcnow().timestamp() - ts_read_post_start) * 1000)
      break
    except botocore.exceptions.ClientError as e:
      if e.response['Error']['Code'] in ['NoSuchKey','404']:
        print(f"[RETRY] Read 'k' v='{k}'", flush=True)
        evaluation['read_post_retries'] += 1
        pass
      else:
        raise

def antipode_bridge(id, role):
  import antipode_s3 as ant # this file will get copied when deploying

  return ant.AntipodeS3(_id=id, conn=_bucket(role))

def clean():
  None