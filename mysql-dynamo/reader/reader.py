import json
import os
import boto3
import pymysql
import pymysql.cursors
from pprint import pprint
from datetime import datetime


#---------------
# AWS SAM Deployment details
#
# Region: us-east-1 (N. Virginia)
#
# Payload Example:
#   - do not forget to invoke the writer first
# { "i": "1", "key": "AABB11", "written_at": "2020-07-13 10:44:29.767250" }
#---------------

MYSQL_HOST = os.environ.get('MYSQL_HOST')
MYSQL_PORT = int(os.environ.get('MYSQL_PORT'))
MYSQL_USER = os.environ.get('MYSQL_USER')
MYSQL_PASSWORD = os.environ.get('MYSQL_PASSWORD')
MYSQL_DB = os.environ.get('MYSQL_DB')
MYSQL_POST_TABLE_NAME = os.environ.get('MYSQL_POST_TABLE_NAME')
SQS_EVAL_QUEUE_URL = os.environ.get('SQS_EVAL_QUEUE_URL')

def lambda_handler(event, context):
  # event example
  # {
  #   'Records': [
  #     {
  #       'awsRegion': 'us-east-1',
  #       'dynamodb': {
  #         'ApproximateCreationDateTime': 1630018407.0,
  #         'Keys': {'k': {'S': '1'}},
  #         'NewImage': {'k': {'S': '1'}, 'v': {'S': 'AABB11'}, 'written_at': {'S': '2021-08-26 23:06:27.735938'}
  #       },
  #       'SequenceNumber': '2084710800000000039455343869',
  #       'SizeBytes': 11,
  #       'StreamViewType': 'NEW_AND_OLD_IMAGES'},
  #       'eventID': '7d15fac5ce0ef335a09a2c9d52993256',
  #       'eventName': 'INSERT',
  #       'eventSource': 'aws:dynamodb',
  #       'eventSourceARN': 'arn:aws:dynamodb:us-east-1:641424397462:table/keyvalue/stream/2020-06-15T14:43:13.249',
  #       'eventVersion': '1.1'
  #     }
  #   ]
  # }

  # if we have an event from SNS topic we parse it
  # otherwise we already receiving an event through standard lambda API
  if 'Records' in event:
    dynamo_event = event['Records'][0]
    if dynamo_event['eventName'] == 'INSERT':
      # pprint(json.dumps(event, default=str))
      event = {
        'i': dynamo_event['dynamodb']['NewImage']['k']['S'],
        'key': dynamo_event['dynamodb']['NewImage']['v']['S'],
        'written_at': dynamo_event['dynamodb']['NewImage']['written_at']['S']
      }
    elif dynamo_event['eventName'] == 'REMOVE':
      return { 'statusCode': 422, 'body': json.dumps(event, default=str) }
    else:
      pprint(event)
      return { 'statusCode': 422, 'body': json.dumps(event, default=str) }

  evaluation = {
    'i': event['i'],
    'ts_notification_spent': (datetime.utcnow() - datetime.strptime(event['written_at'], '%Y-%m-%d %H:%M:%S.%f')).total_seconds(),
    'read_post_retries' : 0,
    'ts_read_post_spent': None,
    'read_post_key_retries' : 0,
    'ts_read_post_key_spent': None,
    'read_post_blob_retries' : 0,
    'ts_read_post_blob_spent': None,
  }

  # connect to mysql
  while True:
    try:
      mysql_conn = pymysql.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        db=MYSQL_DB,
        connect_timeout=30,
        autocommit=True
      )
      break
    except pymysql.Error as e:
      print(f"[ERROR] MySQL exception: {e}")

  # read post
  ts_read_post_key_start = datetime.utcnow()
  while True:
    with mysql_conn.cursor() as cursor:
      sql = f"SELECT `k` FROM `{MYSQL_POST_TABLE_NAME}` WHERE `v`=%s"
      cursor.execute(sql, (event['key'],))
      result = cursor.fetchone()

      # current date and time
      if result is None:
        evaluation['read_post_retries'] += 1
        evaluation['read_post_key_retries'] += 1
        print(f"[RETRY] Read 'k' v='{event['key']}' from MySQL")
      else:
        evaluation['ts_read_post_key_spent'] = (datetime.utcnow() - ts_read_post_key_start).total_seconds()
        break

  ts_read_post_blob_start = datetime.utcnow()
  while True:
    with mysql_conn.cursor() as cursor:
      sql = f"SELECT `b` FROM `{MYSQL_POST_TABLE_NAME}` WHERE `v`=%s"
      cursor.execute(sql, (event['key'],))
      result = cursor.fetchone()

      # current date and time
      if result is None:
        evaluation['read_post_retries'] += 1
        evaluation['read_post_blob_retries'] += 1
        print(f"[RETRY] Read 'b' v='{event['key']}' from MySQL")
      else:
        evaluation['ts_read_post_blob_spent'] = (datetime.utcnow() - ts_read_post_blob_start).total_seconds()
        evaluation['ts_read_post_spent'] = (datetime.utcnow() - ts_read_post_key_start).total_seconds()
        break

  # Due to unknown reasons no events are showing up on SQS when setting up a Lambda destination
  # hence we do it manually here
  cli_sqs = boto3.client('sqs')
  cli_sqs.send_message(
    QueueUrl=SQS_EVAL_QUEUE_URL,
    MessageBody=json.dumps(evaluation, default=str),
  )

  return {
    'statusCode': 200,
    'body': json.dumps(evaluation, default=str)
  }