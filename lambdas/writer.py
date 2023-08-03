import json
import os
from datetime import datetime
import time
import importlib
from context import Context

#--------------
# AWS SAM Deployment details
#
# Lambda payload example:
# { "i": "1", "key": "AABB11", "sent_at": 1630247610.943197 }
#--------------

POST_STORAGE = os.environ['POST_STORAGE']
NOTIFICATION_STORAGE = os.environ['NOTIFICATION_STORAGE']
ANTIPODE = bool(int(os.environ['ANTIPODE']))
DELAY_MS = int(os.environ['DELAY_MS'])

def lambda_handler(event, context):
  # dynamically load methods according to flags
  post_storage_clean = getattr(importlib.import_module(f"{POST_STORAGE}"), 'clean')
  notification_storage_clean = getattr(importlib.import_module(f"{NOTIFICATION_STORAGE}"), 'clean')
  write_notification = getattr(importlib.import_module(f"{NOTIFICATION_STORAGE}"), 'write_notification')
  if ANTIPODE:
    write_post = getattr(importlib.import_module(f"antipode_{POST_STORAGE}"), 'write_post')
  else:
    write_post = getattr(importlib.import_module(f"{POST_STORAGE}"), 'write_post')

  # this is used in cases where Lambdas are inside a VPC and we cannot clean outside of it
  if "-#CLEAN#-" in event:
    # dynamically call clean
    post_storage_clean()
    notification_storage_clean()
    return { 'statusCode': 200, 'body': event }

  #------

  # init context to emulate tracing infra
  context = Context()

  # mark timestamp of start of request processing - for visibility latency
  event['writer_start_at'] = datetime.utcnow().timestamp()

  if ANTIPODE:
    write_post(k=event['key'], c=context)
  else:
    write_post(k=event['key'])

  event['post_written_at'] = datetime.utcnow().timestamp()

  if DELAY_MS > 0:
    time.sleep(DELAY_MS / 1000.0)

  # append context to notification event
  event['context'] = context.to_json()
  # has to be before otherwise we cannot measure in the reader
  event['notification_written_at'] = datetime.utcnow().timestamp()
  write_notification(event)

  # return the event and the code
  return {
      'statusCode': 200,
      'body': json.dumps(event, default=str)
    }