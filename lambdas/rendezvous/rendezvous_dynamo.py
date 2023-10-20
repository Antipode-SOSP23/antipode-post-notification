import os
import boto3

DYNAMO_RENDEZVOUS_TABLE = os.environ['DYNAMO_RENDEZVOUS_TABLE']

def _conn(role):
  region = os.environ[f"{role.upper()}_REGION"]
  return boto3.resource('dynamodb',
      region_name=region,
      endpoint_url=f"http://dynamodb.{region}.amazonaws.com"
    )

def write_post(k, m):
  post_table = _conn('writer').Table(DYNAMO_RENDEZVOUS_TABLE)
  post_table.put_item(Item={
      'k': str(k),
      'b': os.urandom(350000),
      'rv_bid': m,
    })
  
def read_post(k):
  post_table = _conn('reader').Table(DYNAMO_RENDEZVOUS_TABLE)
  # read key of post
  r = post_table.get_item(Key={'k': k}, AttributesToGet=['k'])
  return ('Item' in r)