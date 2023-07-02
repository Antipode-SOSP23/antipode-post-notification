import os
import redis
import json
from datetime import datetime
import time

CACHE_PORT = os.environ['CACHE_PORT']
CACHE_RENDEZVOUS_PREFIX = os.environ['CACHE_RENDEZVOUS_PREFIX']
RENDEZVOUS_METADATA_VALIDITY_S = 1800 # 30 minutes

def _conn(role):
  role = role.upper()
  role_region = os.environ[f"{role}_REGION"]
  host = os.environ[f"CACHE_HOST__{role_region.replace('-','_').upper()}__{role}"]
  return redis.Redis(host=host, port=CACHE_PORT, db=0,
      charset="utf-8", decode_responses=True,
      socket_connect_timeout=5, socket_timeout=5
    )

def _cache_key_rendezvous(bid):
    return CACHE_RENDEZVOUS_PREFIX + ':' + bid

def write_post(i,k):
  op = (k,) # dont forget the comma :)
  post = {
      'i': i,
      'k': k,
      'blob': str(os.urandom(1000000))
    }
  _conn('writer').set(k, json.dumps(post))
  return op

def write_post_rendezvous(i, k, bid):
  op = (k,)
  pipe = _conn('writer').pipeline()
  post = {
    'i': 'i',
    'k': k,
    'blob': str(os.urandom(1000000))
  }
  pipe.set(k, json.dumps(post))
  pipe.set(_cache_key_rendezvous(bid), 1, ex=RENDEZVOUS_METADATA_VALIDITY_S) # integer 1 uses less memory than empty string
  pipe.execute()
  return op

def read_post(k, evaluation):
  r = _conn('reader')
  return bool(r.exists(k))

def antipode_shim(id, role):
  import antipode_cache as ant # this file will get copied when deploying

  return ant.AntipodeCache(_id=id, conn=_conn(role))

def rendezvous_shim(role, service, region):
  import rendezvous_cache as rdv

  return rdv.RendezvousCache(_conn(role), service, region)

def clean():
  # only the writer has permissions to clean
  _conn('writer').flushall()