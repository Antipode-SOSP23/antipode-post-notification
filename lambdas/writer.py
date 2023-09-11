import json
import os
from datetime import datetime
import time
import importlib
import grpc
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
RENDEZVOUS = bool(int(os.environ['RENDEZVOUS']))
DELAY_MS = int(os.environ['DELAY_MS'])

# dynamically load methods according to flags
write_notification = getattr(importlib.import_module(f"{NOTIFICATION_STORAGE}"), 'write_notification')
if ANTIPODE:
  write_post = getattr(importlib.import_module(f"antipode_{POST_STORAGE}"), 'write_post')
  import antipode_core # import after importing module due to wait_registry
elif RENDEZVOUS:
  write_post = getattr(importlib.import_module(f"rendezvous_{POST_STORAGE}"), 'write_post')
  import rendezvous_pb2 as pb, rendezvous_pb2_grpc as pb_grpc
else:
  write_post = getattr(importlib.import_module(f"{POST_STORAGE}"), 'write_post')

def _region(role):
  return os.environ[f"{role.upper()}_REGION"]

def _rendezvous_address(role):
  return os.environ[f"RENDEZVOUS_{_region(role).replace('-','_').upper()}"]

def lambda_handler(event, context):
  # this is used in cases where Lambdas are inside a VPC and we cannot clean outside of it
  if "-#CLEAN#-" in event:
    # dynamically call clean
    getattr(importlib.import_module(f"{POST_STORAGE}"), 'clean')()
    getattr(importlib.import_module(f"{NOTIFICATION_STORAGE}"), 'clean')()
    return { 'statusCode': 200, 'body': event }
  if "-#STATS#-" in event:
    # dynamically call stats
    event['stats'] = getattr(importlib.import_module(f"{POST_STORAGE}"), 'stats')()
    return { 'statusCode': 200, 'body': event }

  if RENDEZVOUS:
    rid = context.aws_request_id

    with grpc.insecure_channel(_rendezvous_address('writer')) as channel:
      stub = pb_grpc.ClientServiceStub(channel)
      try:
        regions = [_region('writer'), _region('reader')]
        request = pb.RegisterBranchMessage(rid=rid, regions=regions, service='post-storage', monitor=True)
        rendezvous_call_start_ts = datetime.utcnow().timestamp()
        response = stub.RegisterBranch(request)
        rendezvous_end_ts = datetime.utcnow().timestamp()
        event['rendezvous_call_writer_spent_ms'] = int((rendezvous_end_ts - rendezvous_call_start_ts) * 1000)
        event['rid'] = rid
        bid = response.bid

      except grpc.RpcError as e:
        print(f"[ERROR] Rendezvous exception registering request request/branches: {e.details()}")
        raise e
  #------

  # init context to emulate tracing infra
  context = Context()

  # mark timestamp of start of request processing - for visibility latency
  event['writer_start_at'] = datetime.utcnow().timestamp()
  if ANTIPODE:
    wid = write_post(k=event['key'], c=context)
    antipode_core.append_operation(context, 'post-storage', wid)
  elif RENDEZVOUS:
    write_post(k=event['key'], m=bid)
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