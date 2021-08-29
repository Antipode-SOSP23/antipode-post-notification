import json
import os
from datetime import datetime
import importlib
import boto3

#---------------
# AWS SAM Deployment details
#
# Lambda payload example: (do not forget to invoke the writer first)
# { "i": "1", "key": "AABB11", "written_at": "2020-07-13 10:44:29.767250" }
#---------------

POST_STORAGE = os.environ['POST_STORAGE']
NOTIFICATION_STORAGE = os.environ['NOTIFICATION_STORAGE']
ANTIPODE = bool(int(os.environ['ANTIPODE']))

def lambda_handler(event, context):
  # dynamically load
  parse_event = getattr(importlib.import_module(NOTIFICATION_STORAGE), 'parse_event')
  read_post = getattr(importlib.import_module(POST_STORAGE), 'read_post')

  # parse event according to the source notification storage
  status_code, event = parse_event(event)
  if status_code != 200:
    return { 'statusCode': status_code, 'body': json.dumps(event, default=str) }

  # init evaluation dict
  evaluation = {
    'i': event['i'],
    'ts_notification_spent': (datetime.utcnow() - datetime.strptime(event['written_at'], '%Y-%m-%d %H:%M:%S.%f')).total_seconds(),
    'read_post_key_retries' : 0,
    'ts_read_post_key_spent': None,
    'read_post_blob_retries' : 0,
    'ts_read_post_blob_spent': None,
    'read_post_retries' : 0,
    'ts_read_post_spent': None,
  }

  # read post and fill evaluation
  read_post(event['key'], evaluation)

  # write evaluation to SQS queue
  cli_sqs = boto3.client('sqs')
  cli_sqs.send_message(
    QueueUrl=os.environ[f"SQS_EVAL_URL__{os.environ['READER_REGION'].replace('-','_').upper()}"],
    MessageBody=json.dumps(evaluation, default=str),
  )

  return { 'statusCode': 200, 'body': json.dumps(evaluation, default=str) }
