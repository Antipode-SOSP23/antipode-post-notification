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

def write_post(k):
  _conn('writer').set(k, str(os.urandom(1000000)))
  wid = (k,) # dont forget the comma :)
  return wid

def read_post(k):
  r = _conn('reader')
  return bool(r.exists(k))

def clean():
  # only the writer has permissions to clean
  _conn('writer').flushall()

def stats():
  from statistics import mean

  conn = _conn('writer')
  stats = {}

  # gather total memory
  r = conn.execute_command('MEMORY STATS')
  stats['dataset.bytes'] = int(r['dataset.bytes'])

  # gather average memory
  stats['dataset.avg_bytes'] = []
  for k in conn.keys():
    r = conn.execute_command(f"MEMORY USAGE {k}")
    stats['dataset.avg_bytes'].append(int(r))
  stats['dataset.avg_bytes'] = mean(stats['dataset.avg_bytes'])

  return stats