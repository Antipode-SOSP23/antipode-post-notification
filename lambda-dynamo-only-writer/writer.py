import json
import os
import boto3
import pymysql
import pymysql.cursors
from pprint import pprint

#---------------
# AWS SAM Deployment details
#
# Region: eu-central-1 (Frankfurt)
# S3 Bucket: antipode-lambda-eu
# Stack name: antipode-lambda-dynamo-only-writer
#
# Payload Example:
# { "i": "1", "key": "AABB11" }
#---------------

def lambda_handler(event, context):
  dynamo_conn = boto3.resource('dynamodb',
      region_name=os.environ['AWS_REGION'],
      endpoint_url=f"http://dynamodb.{os.environ['AWS_REGION']}.amazonaws.com"
    )

  # write post
  post_table = dynamo_conn.Table("blobs")
  post_table.put_item(Item={'k': str(event['key']), 'b': os.urandom(350000) })

  # write notification
  notf_table = dynamo_conn.Table("keyvalue")
  notf_table.put_item(Item={'k': str(event['i']), 'v': str(event['key'])})


  # returns OK with the k,v written
  return {
    'statusCode': 200,
    'body': {
      'key': event['i'],
      'value': event['key'],
    }
  }
