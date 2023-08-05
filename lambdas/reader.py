import json
import os
from datetime import datetime
import importlib
import boto3
from botocore.client import Config
from objsize import get_deep_size
from context import Context

#--------------
# AWS SAM Deployment details
#
# Lambda payload example: (do not forget to invoke the writer first)
#   { "i": "1", "key": "AABB11", "sent_at": 1630247612.943197 }
# or with antipode:
#   { "i": "1", "key": "AABB11", "sent_at": 1630247612.943197, "context": "{\"id\": \"0a61880503354d21aaddee74c11af008\", \"operations\": {\"post_storage\": [[\"blobs\", \"v\", \"AABB11\"]]}}" }
#--------------

POST_STORAGE = os.environ['POST_STORAGE']
NOTIFICATION_STORAGE = os.environ['NOTIFICATION_STORAGE']
ANTIPODE = bool(int(os.environ['ANTIPODE']))
DELAY_MS = int(os.environ['DELAY_MS'])

# dynamically load
parse_event = getattr(importlib.import_module(NOTIFICATION_STORAGE), 'parse_event')
if ANTIPODE:
  read_post = getattr(importlib.import_module(f"antipode_{POST_STORAGE}"), 'read_post')
  import antipode_core
else:
  read_post = getattr(importlib.import_module(f"{POST_STORAGE}"), 'read_post')


def lambda_handler(event, context):
  received_at = datetime.utcnow().timestamp()

  # parse event according to the source notification storage
  status_code, event = parse_event(event)
  if status_code != 200:
    return { 'statusCode': status_code, 'body': json.dumps(event, default=str) }

  # init evaluation dict
  evaluation = {
    # client
    'i': event['i'],
    'client_sent_at': event['client_sent_at'],
    # writer
    'writer_start_at': event['writer_start_at'],
    'post_written_at': event['post_written_at'],
    # reader
    'reader_received_at': received_at,
    'notification_to_reader_spent_ms': int((received_at - event['notification_written_at']) * 1000),
    'post_read_at': None,
    'consistent_read' : 0,
    'antipode_spent_ms': None,
  }

  # init context
  context = Context.from_json(event['context'])

  if ANTIPODE:
    # eval barrier
    antipode_start_ts = datetime.utcnow().timestamp()
    antipode_core.barrier(context)
    evaluation['antipode_spent_ms'] = int((datetime.utcnow().timestamp() - antipode_start_ts) * 1000)

  # Either with or without Antipode we read the same way
  consistent_read = read_post(k=event['key'])

  # read post and fill evaluation
  evaluation['consistent_read'] = int(consistent_read)
  # keep time of read - visibility latency
  evaluation['post_read_at'] = datetime.utcnow().timestamp()
  # measure notification event size in original vs. antipode
  evaluation['notification_size_bytes'] = get_deep_size(event)

  # write evaluation to SQS queue
  # due to bug with VPC and SQS we have to be explicit regarding the endpoint url
  # https://github.com/boto/boto3/issues/1900#issuecomment-471047309
  config = Config(connect_timeout=5, retries={'max_attempts': 5})
  cli_sqs = boto3.Session().client(
      service_name='sqs',
      region_name=os.environ['READER_REGION'],
      endpoint_url=f"https://sqs.{os.environ['READER_REGION']}.amazonaws.com",
      config=config
    )
  cli_sqs.send_message(
      QueueUrl=os.environ[f"SQS_EVAL_URL__{os.environ['READER_REGION'].replace('-','_').upper()}"],
      MessageBody=json.dumps(evaluation, default=str),
    )

  return { 'statusCode': 200, 'body': json.dumps(evaluation, default=str) }
