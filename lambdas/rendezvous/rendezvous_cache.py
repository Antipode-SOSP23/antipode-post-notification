import os
import json
import redis
from rendezvous_shim import RendezvousShim

CACHE_RENDEZVOUS_PREFIX = os.environ['CACHE_RENDEZVOUS_PREFIX']

class RendezvousCache(RendezvousShim):
  def __init__(self, conn, region):
    super().__init__(region)
    self.conn = conn
    self.cursor = 0

  def _parse_metadata(self, item):
    metadata = json.loads(item)
    return metadata['rid'], metadata['service'], metadata['ts']

  def _read_metadata(self):
    try:
      # use redis pipeline to send a single request and avoid multiple round-trips
      pipe = self.conn.pipeline()

      # default count in redis is set to 10 so we must increase it
      # track cursor to continue reading in the next function call and reduce amount of returned items
      self.cursor, keys = self.conn.scan(cursor=self.cursor, match=f'{CACHE_RENDEZVOUS_PREFIX}:*', count=10000)
      for key in keys:
        pipe.get(key)

      return pipe.execute()
    
    except redis.exceptions.RedisError as e:
      print(f"[ERROR] Cache exception reading rendezvous metadata: {e}", flush=True)

    return []
