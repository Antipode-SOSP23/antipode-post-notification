import boto3
import botocore
import os
import json
from datetime import datetime

S3_RENDEZVOUS_PATH = os.environ['S3_RENDEZVOUS_PATH']

def _bucket(role):
  role = role.upper()
  role_region = os.environ[f"{role}_REGION"]
  return os.environ[f"S3_BUCKET__{role_region.replace('-','_').upper()}__{role}"]

def _bucket_key_rendezvous(rid):
  return f"{S3_RENDEZVOUS_PATH}/{rid}"

def write_post(i,k):
  s3_client = boto3.client('s3')
  # we put to reader's bucket on the return because write post has to be read from that bucket
  # this emulates a S3 bucket "cluster" where you can write by a single name
  s3_client.put_object(
      Bucket=_bucket('writer'),
      Key=str(k),
      Body=os.urandom(1000000),
    )
  
def write_post_rendezvous(i, k, rid, bid):
  # s3 does not support transactions so we have to add two distinct objects
  s3_client = boto3.client('s3')

  s3_client.put_object(
    Bucket=_bucket('writer'),
    Key=str(k),
    Body=os.urandom(1000000),
    Metadata={
        'rendezvous': rid
    }
  )

  rendezvous_metadata = {
    'rid': rid,
    'bid': bid,
    'obj_key': str(k)
  }

  s3_client.put_object(
    Bucket=_bucket('writer'),
    Key=_bucket_key_rendezvous(rid),
    Body=json.dumps(rendezvous_metadata)
  )
    
  return (_bucket('reader'), str(k))

def read_post(k, evaluation):
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

def antipode_shim(id, role):
  import antipode_s3 as ant # this file will get copied when deploying

  return ant.AntipodeS3(_id=id, conn=_bucket(role))

def rendezvous_shim(role, region):
  import rendezvous_s3 as rdv

  return rdv.RendezvousS3(_bucket(role), region)

def clean():
  None