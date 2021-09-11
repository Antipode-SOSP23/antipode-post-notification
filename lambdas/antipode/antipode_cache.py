import redis
import antipode as ant

class AntipodeCache:
  def __init__(self, _id, conn):
    self._id = _id
    self.conn = conn

  def _id(self):
    return self._id

  def _cscope_key(self, cid):
    return f"cscope.{cid}"

  def cscope_close(self, c):
    self.conn.set(self._cscope_key(c._id), c.to_json())

  def retrieve_cscope(self, cscope_id, service_registry):
    # read cscope_id
    while True:
      cscope_json = self.conn.get(self._cscope_key(cscope_id))
      if cscope_json is None:
        print(f"[RETRY] Read {self._cscope_key(cscope_id)}", flush=True)
      else:
        return ant.Cscope.from_json(service_registry, cscope_json)

  def cscope_barrier(self, operations):
    # read post operations
    for op in operations:
      # op: (<KEY>)
      while True:
        if self.conn.get(op[0]) is None:
          print(f"[RETRY] Read {op}", flush=True)
        else:
          break
