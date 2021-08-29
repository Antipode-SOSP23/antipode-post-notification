import boto3
import os

DYNAMO_NOTIFICATIONS_TABLE_NAME = os.environ['DYNAMO_NOTIFICATIONS_TABLE_NAME']
ANTIPODE = bool(int(os.environ['ANTIPODE']))

def write_notification(event):
  reader_region = os.environ['READER_REGION']

  # write notification to current AWS region
  table_conn = boto3.resource('dynamodb',
      region_name=reader_region,
      endpoint_url=f"http://dynamodb.{reader_region}.amazonaws.com"
    ).Table(DYNAMO_NOTIFICATIONS_TABLE_NAME)
  # take parts of the event and turn into dynamo notification
  item = {
    'k': str(event['i']),
    'v': str(event['key']),
    'written_at': str(event['written_at']),
  }
  # if antipode is enabled we pass the scope
  if ANTIPODE:
    item['cscope']= event['cscope']
  # write the built item
  table_conn.put_item(Item=item)

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
        'written_at': float(dynamo_event['dynamodb']['NewImage']['written_at']['S'])
      }
      if ANTIPODE:
        event['cscope'] = dynamo_event['dynamodb']['NewImage']['cscope']['S']
    elif dynamo_event['eventName'] == 'REMOVE':
      return 422, event
    else:
      print(f"[ERROR] Unknown event to parse: {event}")
      return 422, event

  return 200, event