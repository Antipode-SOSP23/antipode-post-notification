import boto3
import botocore
import os
from datetime import datetime

def write_post(i,k):
  writer_bucket = os.environ[f"S3_BUCKET__{os.environ['WRITER_REGION'].replace('-','_').upper()}__WRITER"]
  s3_client = boto3.client('s3')
  op = (writer_bucket, str(k))
  s3_client.put_object(
    Bucket=op[0],
    Key=op[1],
    Body=os.urandom(1000000),
  )
  return op

def read_post(k, evaluation):
  reader_bucket = os.environ[f"S3_BUCKET__{os.environ['READER_REGION'].replace('-','_').upper()}__READER"]
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
      s3_client.get_object(Bucket=reader_bucket, Key=str(k))
      evaluation['ts_read_post_spent_ms'] = int((datetime.utcnow().timestamp() - ts_read_post_start) * 1000)
      break
    except botocore.exceptions.ClientError as e:
      if e.response['Error']['Code'] == 'NoSuchKey':
        print(f"[RETRY] Read 'k' v='{k}'")
        evaluation['read_post_retries'] += 1
        pass
      else:
        raise

def antipode_bridge(id, role):
  import antipode_s3 as ant # this file will get copied when deploying

  role = role.upper()
  role_region = os.environ[f"{role}_REGION"]
  bucket = os.environ[f"S3_BUCKET__{role_region.replace('-','_').upper()}__{role}"]
  return ant.AntipodeS3(_id=id, conn=bucket)