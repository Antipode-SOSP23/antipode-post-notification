import json
import os
import boto3
from botocore.exceptions import ClientError
import pymysql
import pymysql.cursors
from pprint import pprint
from datetime import datetime


#---------------
# AWS SAM Deployment details
#
# Region: us-east-1 (N. Virginia)
# S3 Bucket: antipode-lambda-us
# Stack name: antipode-lambda-dynamo-only-reader
#
# Payload Example:
# { "i": "1", "key": "AABB11" }
#---------------

def lambda_handler(event, context):
  pprint(event)
  pprint(context)
  reply = {
    'role': 'reader',
    'key': event['i'],
    'value': event['key'],
    'writer_reply': {},
    'evaluation': {
      'ts_start': None,
      'ts_writer_reply': None,
      'read_notf_retries' : 0,
      'read_post_retries' : 0,
      'ts_read_notf_start': None,
      'ts_read_notf_exit': None,
      'ts_read_post_exit': None,
    }
  }

  reply['evaluation']['ts_start'] = datetime.now()

  writer_client = boto3.client('lambda', region_name='eu-central-1')
  response = writer_client.invoke(
    FunctionName='arn:aws:lambda:eu-central-1:641424397462:function:antipode-lambda-dynamo-on-antipodelambdadynamoonly-HJAZ1W3XI1R0',
    InvocationType='RequestResponse',
    Payload=json.dumps(event),
  )

  reply['evaluation']['ts_writer_reply'] = datetime.now()

  body = json.loads(response['Payload'].read()).get('body', {})
  if not body:
    print("[ERROR] Empty reply!")
    return {
      'statusCode': 500
    }
  else:
    reply['writer_reply'] = body

    dynamo_conn = boto3.resource('dynamodb',
        region_name=os.environ['AWS_REGION'],
        endpoint_url=f"http://dynamodb.{os.environ['AWS_REGION']}.amazonaws.com"
      )

    # read notification
    print("[INFO] Reading notification", end='')
    reply['evaluation']['ts_read_notf_start'] = datetime.now()

    notf_table = dynamo_conn.Table("keyvalue")
    while True:
      try:
        print('', end ='...')
        if 'Item' in notf_table.get_item(Key={'k': str(event['i'])}):
          break
        else:
          reply['evaluation']['read_notf_retries'] += 1
      except ClientError as err:
        print(f"[ERROR] Exception while getting item: {e}")
        pass

    reply['evaluation']['ts_read_notf_exit'] = datetime.now()
    print("[INFO] Done!")

    # read post
    print("[INFO] Reading post", end='')

    post_table = dynamo_conn.Table("blobs")
    while True:
      try:
        print('', end ='...')
        if 'Item' in post_table.get_item(Key={'k': str(event['key'])}):
          break
        else:
          reply['evaluation']['read_post_retries'] += 1
      except ClientError as err:
        print(f"[ERROR] Exception while getting item: {e}")
        pass

    reply['evaluation']['ts_read_post_exit'] = datetime.now()
    print("[INFO] Done!")


    return {
      'statusCode': 200,
      'body': json.dumps(reply, default=str),
    }
