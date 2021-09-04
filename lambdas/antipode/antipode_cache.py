import redis

class AntipodeCache:
  def __init__(self, _id, conn):
    self._id = _id
    self.conn = conn

  def _id(self):
    return self._id

  def _cscope_key(self, cid):
    return f"cscope.{cid}"

  def cscope_close(self, c):
    # we could add more info as json here
    self.conn.set(self._cscope_key(c._id), '')

  def cscope_barrier(self, cscope_id, operations):
    # read cscope_id
    while True:
      if self.conn.get(self._cscope_key(cscope_id)) is None:
        print(f"[RETRY] Read {self._cscope_key(cscope_id)}", flush=True)
      else:
        break
    # read post operations
    for op in operations:
      # op: (<KEY>)
      while True:
        if self.conn.get(op[0]) is None:
          print(f"[RETRY] Read {op}", flush=True)
        else:
          break
