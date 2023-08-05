import os
import boto3
from boto3.dynamodb.conditions import Key, Attr

# Using same table as the non-antipode version
DYNAMO_ANTIPODE_TABLE = os.environ['DYNAMO_POST_TABLE_NAME']

def _conn(role):
  region = os.environ[f"{role.upper()}_REGION"]
  return boto3.resource('dynamodb',
      region_name=region,
      endpoint_url=f"http://dynamodb.{region}.amazonaws.com"
    )

def write_post(k, c):
  post_table = _conn('writer').Table(DYNAMO_ANTIPODE_TABLE)
  post_table.put_item(Item={
      'k': str(k),
      'cid': str(c._id),
      'b': os.urandom(350000),
    })
  wid = (k,)
  return wid

def wait(cid, operations):
  post_table = _conn('reader').Table(DYNAMO_ANTIPODE_TABLE)
  # read all keys in context
  for (k,) in operations:
    while True:
      # -- Option 1 : Scan for cid
      # Even though we could use the key directly, to better assess
      # Antipode's performance we use a scan by cid first
      # r = post_table.scan(Select='SPECIFIC_ATTRIBUTES',
      #   ProjectionExpression='k',
      #   FilterExpression=Attr('cid').eq(cid))
      # If we find the context id in the scan, and the object key in the wait
      # found = any([i['k'] == k for i in r.get('Items', [])])

      # -- Option 2 : Strong read
      r = post_table.get_item(Key={'k': k}, AttributesToGet=['k'], ConsistentRead=True)
      found = ('Item' in r)

      # -- Option 3: Implement version keys and read the key and version
      # https://aws.amazon.com/blogs/database/implementing-version-control-using-amazon-dynamodb/
      #

      if found: break

def read_post(k):
  post_table = _conn('reader').Table(DYNAMO_ANTIPODE_TABLE)
  # read key of post
  r = post_table.get_item(Key={'k': k}, AttributesToGet=['k'])
  return ('Item' in r)