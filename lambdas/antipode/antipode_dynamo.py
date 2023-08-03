import os
import boto3

DYNAMO_ANTIPODE_TABLE = os.environ['DYNAMO_ANTIPODE_TABLE']

def _conn(role):
  region = os.environ[f"{role.upper()}_REGION"]
  return boto3.resource('dynamodb',
      region_name=region,
      endpoint_url=f"http://dynamodb.{region}.amazonaws.com"
    )

def write_post(k, c):
  post_table = _conn('writer').Table(DYNAMO_ANTIPODE_TABLE)
  op = (str(k), str(c._id))
  post_table.put_item(Item={
      'key': str(k),
      'context_id': str(c._id),
      'b': os.urandom(350000),
    })
  antipode_core.append_operation(c, 'post-storage', op)
  return op

def wait(operations):
  post_table = _conn('reader').Table(DYNAMO_ANTIPODE_TABLE)
  # read all keys in context
  for op in operations:
    while True:
      if 'Item' in post_table.get_item(Key={'key': op[0], 'context_id': op[1]}, AttributesToGet=['key']):
        break

def read_post(k, c):
  post_table = _conn('reader').Table(DYNAMO_ANTIPODE_TABLE)
  # read key of post
  r = post_table.get_item(Key={'key': k, 'context_id': c._id}, AttributesToGet=['k'])
  return ('Item' in r)

##
# Keep this import at the end so all methods are defined when Antipode's wait register is called
import antipode_core