import json
import os
from datetime import datetime
import importlib
import boto3

#--------------
# AWS SAM Deployment details
#
# Lambda payload example: (do not forget to invoke the writer first)
#   { "i": "1", "key": "AABB11", "written_at": 1630247612.943197 }
# or with antipode:
#   { "i": "1", "key": "AABB11", "written_at": 1630247612.943197, "cscope": "{\"id\": \"0a61880503354d21aaddee74c11af008\", \"operations\": {\"post_storage\": [[\"blobs\", \"v\", \"AABB11\"]]}}" }
#--------------

POST_STORAGE = os.environ['POST_STORAGE']
NOTIFICATION_STORAGE = os.environ['NOTIFICATION_STORAGE']
ANTIPODE = bool(int(os.environ['ANTIPODE']))
DELAY_MS = int(os.environ['DELAY_MS'])

def lambda_handler(event, context):
  # dynamically load
  parse_event = getattr(importlib.import_module(NOTIFICATION_STORAGE), 'parse_event')
  read_post = getattr(importlib.import_module(POST_STORAGE), 'read_post')
  antipode_bridge = getattr(importlib.import_module(POST_STORAGE), 'antipode_bridge')

  received_at = datetime.utcnow().timestamp()

  # parse event according to the source notification storage
  status_code, event = parse_event(event)
  if status_code != 200:
    return { 'statusCode': status_code, 'body': json.dumps(event, default=str) }

  # init evaluation dict
  evaluation = {
    'i': event['i'],
    'sent_at': event['sent_at'],
    'ts_notification_spent_ms': int((received_at - event['written_at']) * 1000),
    'read_post_retries' : 0,
    'ts_read_post_spent_ms': None,
    'antipode_spent_ms': None,
  }

  if ANTIPODE:
    # eval antipode
    antipode_start_ts = datetime.utcnow().timestamp()

    # import antipode lib
    import antipode as ant
    # init service registry
    SERVICE_REGISTRY = {
      'post_storage': antipode_bridge('post_storage', 'reader')
    }
    # deserialize cscope
    cscope = ant.Cscope.from_json(SERVICE_REGISTRY, event['cscope'])
    # barrier and set eval
    cscope.barrier()
    evaluation['antipode_spent_ms'] = int((datetime.utcnow().timestamp() - antipode_start_ts) * 1000)

  # read post and fill evaluation
  read_post(event['key'], evaluation)

  evaluation['post_read_at'] = datetime.utcnow().timestamp()

  # write evaluation to SQS queue
  cli_sqs = boto3.client('sqs')
  cli_sqs.send_message(
    QueueUrl=os.environ[f"SQS_EVAL_URL__{os.environ['READER_REGION'].replace('-','_').upper()}"],
    MessageBody=json.dumps(evaluation, default=str),
  )

  return { 'statusCode': 200, 'body': json.dumps(evaluation, default=str) }
