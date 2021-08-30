import boto3
import botocore
import os
from datetime import datetime

ANTIPODE = bool(int(os.environ['ANTIPODE']))
WRITER_BUCKET = os.environ[f"bucket__{os.environ['WRITER_REGION']}__writer"]
READER_BUCKET = os.environ[f"bucket__{os.environ['READER_REGION']}__reader"]

def write_post(i,k):
  s3_client = boto3.client('s3')
  op = (WRITER_BUCKET, str(k))
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
      s3_client.get_object(Bucket=READER_BUCKET, Key=str(k))
      evaluation['ts_read_post_spent_ms'] = int((datetime.utcnow().timestamp() - ts_read_post_start) * 1000)
      break
    except botocore.exceptions.ClientError as e:
      if e.response['Error']['Code'] == 'NoSuchKey':
        # print(f"[RETRY] Read 'k' v='{k}'")
        evaluation['read_post_retries'] += 1
        pass
      else:
        raise

def antipode_bridge(id, role):
  import antipode_s3 as ant # this file will get copied when deploying

  return ant.AntipodeS3(_id=id, conn=READER_BUCKET)