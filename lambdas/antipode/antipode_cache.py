import os
import redis

CACHE_PORT = os.environ['CACHE_PORT']

def _conn(role):
  role = role.upper()
  role_region = os.environ[f"{role}_REGION"]
  host = os.environ[f"CACHE_HOST__{role_region.replace('-','_').upper()}__{role}"]
  return redis.Redis(host=host, port=CACHE_PORT, db=0,
      charset="utf-8", decode_responses=True,
      socket_connect_timeout=5, socket_timeout=5
    )

def write_post(k, c):
  # Execute this in a transaction so both keys are replicated at the same time
  # With this setup we also ensure quicker reads either k or context id
  tx = _conn('writer').pipeline()
  tx.set(k, str(os.urandom(1000000)))
  tx.set(c._id, k)
  tx.execute()
  #
  op = (c._id,k)
  return op

def wait(operations):
  r = _conn('reader')
  # read context operations
  # wid -> k
  for (cid,wid) in operations:
    while True:
      if bool(r.exists(cid)) and bool(r.exists(wid)):
        break
      else:
        print(f"[RETRY] Read {cid}", flush=True)

def read_post(k):
  r = _conn('reader')
  return bool(r.exists(k))

##
# Keep this import at the end so all methods are defined when Antipode's wait register is called
import antipode_core