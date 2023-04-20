import os
import json
from rendezvous_shim import RendezvousShim

CACHE_RENDEZVOUS_PREFIX = os.environ['CACHE_RENDEZVOUS_PREFIX']

class RendezvousCache(RendezvousShim):
  def __init__(self, conn, region):
    super().__init__(region)
    self.conn = conn
    self.cursor = 0

  def _cache_key_rendezvous(self, rid):
    return f"{CACHE_RENDEZVOUS_PREFIX}:{rid}"
  
  def _cache_prefix_rendezvous(self):
    return f"{CACHE_RENDEZVOUS_PREFIX}:*"

# ----------------
# Current request
# ----------------

  def read_metadata(self, rid):
    while True:
      item = self.conn.get(self._cache_key_rendezvous(rid))
      if item:
        metadata = json.loads(item)
        return metadata['bid']
      
      self.inconsistency = True

# -------------
# All requests
# -------------

  def _parse_metadata(self, item):
    metadata = json.loads(item)
    return metadata['rid'], metadata['bid']

  def read_all_metadata(self):
    pipe = self.conn.pipeline()

    # track cursor to continue reading in the next function call
    self.cursor, keys = self.conn.scan(cursor=self.cursor, match=self._cache_prefix_rendezvous(), count=10000)
    for key in keys:
      pipe.get(key)

    return pipe.execute()
