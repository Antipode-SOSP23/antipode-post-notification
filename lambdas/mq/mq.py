import stomp
import json
import os

MQ_HOST = os.environ[f"MQ_STOMP_HOST__{os.environ['WRITER_REGION'].replace('-','_').upper()}"]
MQ_PORT = os.environ[f"MQ_STOMP_PORT__{os.environ['WRITER_REGION'].replace('-','_').upper()}"]
MQ_USER = os.environ['MQ_USER']
MQ_PASSWORD = os.environ['MQ_PASSWORD']
MQ_NOTIFICATION_QUEUE = os.environ['MQ_NOTIFICATION_QUEUE']

def write_notification(event):
  host = [(MQ_HOST, MQ_PORT)]
  conn = stomp.Connection(host)
  conn.set_ssl(host)
  conn.connect(MQ_USER, MQ_PASSWORD, wait=True)
  conn.send(body=json.dumps(event), destination='antipode-notifications')
  conn.disconnect()

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