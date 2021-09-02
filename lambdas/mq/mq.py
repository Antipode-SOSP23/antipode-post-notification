import stomp
import json
import os

MQ_PORT = os.environ['MQ_STOMP_PORT']
MQ_USER = os.environ['MQ_USER']
MQ_PASSWORD = os.environ['MQ_PASSWORD']
MQ_NOTIFICATION_QUEUE = os.environ['MQ_NOTIFICATION_QUEUE']

def _conn(role):
  role_region = os.environ[f"{role.upper()}_REGION"]
  role_host = os.environ[f"MQ_STOMP_HOST__{role_region.replace('-','_').upper()}"]

  host = [(role_host, MQ_PORT)]
  print(host, flush=True)
  c = stomp.Connection(host)
  c.set_ssl(host)
  c.connect(MQ_USER, MQ_PASSWORD, wait=False)
  return c

def write_notification(event):
  c = _conn('writer')
  c.send(body=json.dumps(event), destination=MQ_NOTIFICATION_QUEUE)
  c.disconnect()

def parse_event(event):
  import base64

  # if we have an event from a source we parse it
  # otherwise we already receiving an event through test lambda API
  if 'messages' in event:
    # messages come in base64 format
    b64_data = event['messages'][0]['data']
    # decode b64 and load json
    event = json.loads(base64.b64decode(b64_data.encode('ascii')).decode('ascii'))

  return 200, event