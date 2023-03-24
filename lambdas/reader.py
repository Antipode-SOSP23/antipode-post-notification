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
RENDEZVOUS_ADDRESS = os.environ['RENDEZVOUS_ADDRESS']
DELAY_MS = int(os.environ['DELAY_MS'])

def _region(role):
  return os.environ[f"{role.upper()}_REGION"]

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
    import rendezvous as rdv, rendezvous_pb2 as rdv_pb, rendezvous_pb2_grpc as rdv_service
    
    # start thread that will close all branches in reader region
    SERVICE_REGISTRY = rendezvous_shim('reader', _region('reader'))
    rendezvous = rdv.Rendezvous(SERVICE_REGISTRY)
    rendezvous.init_polling()

    channel = grpc.insecure_channel(RENDEZVOUS_ADDRESS)
    stub = rdv_service.RendezvousServiceStub(channel)
      
    try:
      # track time to measure inconsistency window
      rendezvous_start_ts = datetime.utcnow().timestamp()
      response = stub.waitRequest(rdv_pb.WaitRequestMessage(rid=event['rid']), timeout=60) # timeout of 60 seconds

      # rendezvous evaluation for prevented inconsistencies
      if response.preventedInconsistency:
        evaluation['rendezvous_inconsistency_window_ms'] = int((datetime.utcnow().timestamp() - rendezvous_start_ts) * 1000)
        evaluation['rendezvous_prevented_inconsistency'] = 1
          
    except grpc.RpcError as e:
      print(f"[ERROR] Rendezvous exception waiting request: {e.details()}")

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
  print("Sending message:", evaluation)
  cli_sqs.send_message(
      QueueUrl=os.environ[f"SQS_EVAL_URL__{os.environ['READER_REGION'].replace('-','_').upper()}"],
      MessageBody=json.dumps(evaluation, default=str),
    )

  return { 'statusCode': 200, 'body': json.dumps(evaluation, default=str) }
