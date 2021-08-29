import json
import os
from datetime import datetime
import importlib

#---------------
# AWS SAM Deployment details
#
# Lambda payload example:
# { "i": "1", "key": "AABB11" }
#---------------

POST_STORAGE = os.environ['POST_STORAGE']
NOTIFICATION_STORAGE = os.environ['NOTIFICATION_STORAGE']
ANTIPODE = bool(int(os.environ['ANTIPODE']))

def lambda_handler(event, context):
  # dynamically load
  write_post = getattr(importlib.import_module(POST_STORAGE), 'write_post')
  write_notification = getattr(importlib.import_module(NOTIFICATION_STORAGE), 'write_notification')

  op = write_post(i=event['i'], k=event['key'])
  event['written_at'] = str(datetime.utcnow())
  write_notification(event)

  # return the event and the code
  return {
    'statusCode': 200,
    'body': json.dumps(event, default=str)
  }