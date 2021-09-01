import boto3
import json
import os

SNS_ARN = os.environ[f"SNS_ARN__{os.environ['WRITER_REGION'].replace('-','_').upper()}__WRITER"]

def write_notification(event):
  boto3.client('sns', region_name=os.environ['WRITER_REGION']).publish(
      TargetArn=SNS_ARN,
      Message=json.dumps(event)
    )

def parse_event(event):
  # if we have an event from a source we parse it
  # otherwise we already receiving an event through test lambda API
  if 'Records' in event:
    event = json.loads(event['Records'][0]['Sns']['Message'])

  return 200, event