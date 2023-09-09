import json
import os
from datetime import datetime
import time
import importlib
import grpc

#--------------
# AWS SAM Deployment details
#
# Lambda payload example:
# { "i": "1", "key": "AABB11", "sent_at": 1630247610.943197 }
#--------------

POST_STORAGE = os.environ['POST_STORAGE']
NOTIFICATION_STORAGE = os.environ['NOTIFICATION_STORAGE']
ANTIPODE = bool(int(os.environ['ANTIPODE']))
ANTIPODE_RENDEZVOUS_ENABLED = bool(int(os.environ['ANTIPODE_RENDEZVOUS_ENABLED']))
RENDEZVOUS = bool(int(os.environ['RENDEZVOUS']))
DELAY_MS = int(os.environ['DELAY_MS'])

def _region(role):
  return os.environ[f"{role.upper()}_REGION"]

def _rendezvous_address(role):
  return os.environ[f"RENDEZVOUS_{_region(role).replace('-','_').upper()}"]

def lambda_handler(event, context):
  # this is used in cases where Lambdas are inside a VPC and we cannot clean outside of it
  if "-#CLEAN#-" in event:
    # dynamically call clean
    getattr(importlib.import_module(POST_STORAGE), 'clean')()
    getattr(importlib.import_module(NOTIFICATION_STORAGE), 'clean')()
    return { 'statusCode': 200, 'body': event }

  # dynamically load
  write_post = getattr(importlib.import_module(POST_STORAGE), 'write_post')
  write_post_rendezvous = getattr(importlib.import_module(POST_STORAGE), 'write_post_rendezvous')
  write_notification = getattr(importlib.import_module(NOTIFICATION_STORAGE), 'write_notification')
  antipode_shim = getattr(importlib.import_module(POST_STORAGE), 'antipode_shim')

  # init Antipode service registry and request context
  if ANTIPODE:
    import antipode as ant
    SERVICE_REGISTRY = {
      'post_storage': antipode_shim('post_storage', 'writer')
    }
    cscope = ant.Cscope(SERVICE_REGISTRY)

  if RENDEZVOUS:
    import rendezvous_pb2 as pb, rendezvous_pb2_grpc as pb_grpc
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

  # mark timestamp of start of request processing - for visibility latency

  event['writer_start_at'] = datetime.utcnow().timestamp()
  if RENDEZVOUS:
    op = write_post_rendezvous(i=event['i'], k=event['key'], bid=bid)
  else:
    op = write_post(i=event['i'], k=event['key'])
  event['post_written_at'] = datetime.utcnow().timestamp()

  if ANTIPODE:
    cscope.append('post_storage', op)
    if ANTIPODE_RENDEZVOUS_ENABLED:
      cscope.close()
    event['cscope'] = cscope.to_json()

  if DELAY_MS > 0:
    time.sleep(DELAY_MS / 1000.0)

  # has to be before otherwise we cannot measure in the reader
  event['notification_written_at'] = datetime.utcnow().timestamp()
  write_notification(event)

  # return the event and the code
  return {
      'statusCode': 200,
      'body': json.dumps(event, default=str)
    }