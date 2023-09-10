import os
import boto3

DYNAMO_POST_TABLE_NAME = os.environ['DYNAMO_POST_TABLE_NAME']

def _conn(role):
  region = os.environ[f"{role.upper()}_REGION"]
  return boto3.resource('dynamodb',
      region_name=region,
      endpoint_url=f"http://dynamodb.{region}.amazonaws.com"
    )

def write_post(k, m):
  post_table = _conn('writer').Table(DYNAMO_POST_TABLE_NAME)
  post_table.put_item(Item={
      'k': str(k),
      'b': os.urandom(350000),
      'rdv_bid': m,
    })