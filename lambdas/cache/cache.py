import os
import redis
import json
from datetime import datetime

CACHE_PORT = os.environ['CACHE_PORT']

def _conn(role):
  role = role.upper()
  role_region = os.environ[f"{role}_REGION"]
  host = os.environ[f"CACHE_HOST__{role_region.replace('-','_').upper()}__{role}"]
  return redis.Redis(host=host, port=CACHE_PORT, db=0,
      charset="utf-8", decode_responses=True,
      socket_connect_timeout=5, socket_timeout=5
    )

def write_post(i,k):
  op = (k,) # dont forget the comma :)
  post = {
      'i': i,
      'k': k,
      'blob': str(os.urandom(1000000))
    }
  _conn('writer').set(k, json.dumps(post))
  return op

def read_post(k, evaluation):
  print("READ POST", flush=True)
  r = _conn('reader')

  # evaluation keys to fill
  # {
  #   'read_post_retries' : 0,
  #   'ts_read_post_spent_ms': None,
  # }

  # read key of post
  ts_read_post_start = datetime.utcnow().timestamp()
  while True:
    if bool(r.exists(k)):
      evaluation['ts_read_post_spent_ms'] = int((datetime.utcnow().timestamp() - ts_read_post_start) * 1000)
      break
    else:
      evaluation['read_post_retries'] += 1
      print(f"[RETRY] Read 'k' v='{k}'", flush=True)

def antipode_bridge(id, role):
  import antipode_cache as ant # this file will get copied when deploying

  return ant.AntipodeCache(_id=id, conn=_conn(role))

def clean():
  for role in ['writer', 'reader']:
    _conn(role).flushall()