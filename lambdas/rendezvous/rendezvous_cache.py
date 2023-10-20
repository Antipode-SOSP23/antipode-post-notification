import os
import redis

# 30 minutes metadata expiration
RENDEZVOUS_METADATA_VALIDITY_S = 1800
CACHE_PORT = os.environ['CACHE_PORT']
CACHE_RENDEZVOUS_PREFIX = os.environ['CACHE_RENDEZVOUS_PREFIX']

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

def write_post(k, m):
  pipe = _conn('writer').pipeline()
  pipe.set(k, str(os.urandom(1000000)))
  # set rendezvous value as 1 to minimize memory used
  pipe.set(_cache_key_rendezvous(m), 1, ex=RENDEZVOUS_METADATA_VALIDITY_S)
  pipe.execute()

def read_post(k):
  r = _conn('reader')
  return bool(r.exists(k))