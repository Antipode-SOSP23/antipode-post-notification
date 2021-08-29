import boto3
import json
import os

def write_notification(event):
  boto3.client('sns').publish(
    TargetArn=os.environ[f"SNS_ARN__{os.environ['WRITER_REGION'].replace('-','_').upper()}__WRITER"],
    Message=json.dumps(event)
  )

def parse_event(event):
  # if we have an event from a source we parse it
  # otherwise we already receiving an event through test lambda API
  if 'Records' in event:
    event = json.loads(event['Records'][0]['Sns']['Message'])

  return 200, event