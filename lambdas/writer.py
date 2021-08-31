import json
import os
from datetime import datetime
import time
import importlib

#--------------
# AWS SAM Deployment details
#
# Lambda payload example:
# { "i": "1", "key": "AABB11" }
#--------------

POST_STORAGE = os.environ['POST_STORAGE']
NOTIFICATION_STORAGE = os.environ['NOTIFICATION_STORAGE']
ANTIPODE = bool(int(os.environ['ANTIPODE']))
DELAY_MS = int(os.environ['DELAY_MS'])

def lambda_handler(event, context):
  # dynamically load
  write_post = getattr(importlib.import_module(POST_STORAGE), 'write_post')
  write_notification = getattr(importlib.import_module(NOTIFICATION_STORAGE), 'write_notification')
  antipode_bridge = getattr(importlib.import_module(POST_STORAGE), 'antipode_bridge')

  if ANTIPODE:
    import antipode as ant
    # init service registry
    SERVICE_REGISTRY = {
      'post_storage': antipode_bridge('post_storage', 'writer')
    }
    cscope = ant.Cscope(SERVICE_REGISTRY)

  op = write_post(i=event['i'], k=event['key'])
  event['written_at'] = datetime.utcnow().timestamp()

  if ANTIPODE:
    cscope.append('post_storage', op)
    cscope.close()
    event['cscope'] = cscope.to_json()

  if DELAY_MS > 0:
    time.sleep(DELAY_MS / 1000.0)

  write_notification(event)

  # return the event and the code
  return {
    'statusCode': 200,
    'body': json.dumps(event, default=str)
  }