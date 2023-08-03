import boto3
import os
from datetime import datetime

DYNAMO_NOTIFICATIONS_TABLE_NAME = os.environ['DYNAMO_NOTIFICATIONS_TABLE_NAME']
DYNAMO_POST_TABLE_NAME = os.environ['DYNAMO_POST_TABLE_NAME']
ANTIPODE = bool(int(os.environ['ANTIPODE']))

def _conn(role):
  region = os.environ[f"{role.upper()}_REGION"]
  return boto3.resource('dynamodb',
      region_name=region,
      endpoint_url=f"http://dynamodb.{region}.amazonaws.com"
    )

def write_post(k):
  post_table = _conn('writer').Table(DYNAMO_POST_TABLE_NAME)
  op = (DYNAMO_POST_TABLE_NAME, 'k', k)
  post_table.put_item(Item={
      'k': str(k),
      'b': os.urandom(350000),
    })
  return op

def read_post(k, evaluation):
  post_table = _conn('reader').Table(DYNAMO_POST_TABLE_NAME)
  # read key of post
  return ('Item' in post_table.get_item(Key={'k': str(k)}, AttributesToGet=['k']))

def write_notification(event):
  # write notification to current AWS region
  notifications_table = _conn('writer').Table(DYNAMO_NOTIFICATIONS_TABLE_NAME)
  # take parts of the event and turn into dynamo notification
  item = {
    'k': str(event['i']),
    'v': str(event['key']),
    'client_sent_at': str(event['client_sent_at']),
    'writer_start_at': str(event['writer_start_at']),
    'post_written_at': str(event['post_written_at']),
    'notification_written_at': str(event['notification_written_at']),
  }
  # if antipode is enabled we pass the scope
  if ANTIPODE:
    item['cscope']= event['cscope']
  # write the built item
  notifications_table.put_item(Item=item)

def parse_event(event):
  # if we have an event from a source we parse it
  # otherwise we already receiving an event through test lambda API
  if 'Records' in event:
    # events in dynamo stream have different names
    # we are only interested in INSERT events and return error for all others
    dynamo_event = event['Records'][0]
    if dynamo_event['eventName'] == 'INSERT':
      event = {
        'i': dynamo_event['dynamodb']['NewImage']['k']['S'],
        'key': dynamo_event['dynamodb']['NewImage']['v']['S'],
        'client_sent_at': float(dynamo_event['dynamodb']['NewImage']['client_sent_at']['S']),
        'writer_start_at': float(dynamo_event['dynamodb']['NewImage']['writer_start_at']['S']),
        'post_written_at': float(dynamo_event['dynamodb']['NewImage']['post_written_at']['S']),
        'notification_written_at': float(dynamo_event['dynamodb']['NewImage']['notification_written_at']['S']),
      }
      if ANTIPODE:
        event['cscope'] = dynamo_event['dynamodb']['NewImage']['cscope']['S']
    elif dynamo_event['eventName'] == 'REMOVE':
      return 422, event
    else:
      print(f"[ERROR] Unknown event to parse: {event}")
      return 422, event

  return 200, event

def clean():
  None