import json
import os
from datetime import datetime
import importlib
import boto3
from botocore.client import Config
import grpc

#--------------
# AWS SAM Deployment details
#
# Lambda payload example: (do not forget to invoke the writer first)
#   { "i": "1", "key": "AABB11", "sent_at": 1630247612.943197 }
# or with antipode:
#   { "i": "1", "key": "AABB11", "sent_at": 1630247612.943197, "cscope": "{\"id\": \"0a61880503354d21aaddee74c11af008\", \"operations\": {\"post_storage\": [[\"blobs\", \"v\", \"AABB11\"]]}}" }
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
  # dynamically load
  parse_event = getattr(importlib.import_module(NOTIFICATION_STORAGE), 'parse_event')
  read_post = getattr(importlib.import_module(POST_STORAGE), 'read_post')
  antipode_shim = getattr(importlib.import_module(POST_STORAGE), 'antipode_shim')
  rendezvous_shim = getattr(importlib.import_module(POST_STORAGE), 'rendezvous_shim')

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
    'rendezvous_spent_ms': None,
    'rendezvous_inconsistency_window_ms': None,
    'rendezvous_prevented_inconsistency': 0,
  }

  if ANTIPODE:
    # eval antipode
    antipode_start_ts = datetime.utcnow().timestamp()

    # import antipode lib
    import antipode as ant
    # init service registry
    SERVICE_REGISTRY = {
      'post_storage': antipode_shim('post_storage', 'reader')
    }
    # deserialize cscope
    cscope = ant.Cscope.from_json(SERVICE_REGISTRY, event['cscope'])
    if ANTIPODE_RENDEZVOUS_ENABLED:
      cscope.rendezvous_barrier()
    else:
      cscope.barrier()

    evaluation['antipode_spent_ms'] = int((datetime.utcnow().timestamp() - antipode_start_ts) * 1000)

  if RENDEZVOUS:
    rendezvous_start_ts = datetime.utcnow().timestamp()

    import rendezvous as rdv
    rid = event['rid']
    
    # start thread that will close the branch for the current region
    shim_layer = rendezvous_shim('reader', _region('reader'))
    rendezvous = rdv.Rendezvous(shim_layer)
    rendezvous.init_close_branch(rid)

    import rendezvous_pb2 as rdv_proto, rendezvous_pb2_grpc as rdv_service

    channel = grpc.insecure_channel(_rendezvous_address('reader'))
    stub = rdv_service.RendezvousServiceStub(channel)
      
    try:
      rendezvous_context = rdv.context_string_to_proto(event['rendezvous_context'])
      request = rdv_proto.WaitRequestMessage(rid=rid, region=_region('reader'))
      request.context.CopyFrom(rendezvous_context)

      rendezvous_wait_start_ts = datetime.utcnow().timestamp()
      stub.waitRequest(request, timeout=60) # timeout of 60 seconds 
      rendezvous_end_ts = datetime.utcnow().timestamp()

      # rendezvous evaluation for prevented inconsistencies
      if rendezvous.prevented_inconsistency():
        evaluation['rendezvous_inconsistency_window_ms'] = int((rendezvous_end_ts - rendezvous_wait_start_ts) * 1000)
        evaluation['rendezvous_prevented_inconsistency'] = 1
      
      evaluation['rendezvous_spent_ms'] = int((rendezvous_end_ts - rendezvous_start_ts) * 1000)
          
    except grpc.RpcError as e:
      print(f"[ERROR] Rendezvous exception waiting request: {e.details()}")
      return { 'statusCode': 500, 'body': json.dumps(evaluation, default=str) }

  # read post and fill evaluation
  evaluation['consistent_read'] = int(read_post(event['key'], evaluation))
  # keep time of read - visibility latency
  evaluation['post_read_at'] = datetime.utcnow().timestamp()

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
