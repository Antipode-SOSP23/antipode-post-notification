import uuid
import json

class Cscope:
  def __init__(self, service_registry, c=None):
    self.service_registry = service_registry

    if c is None:
      self.c = {
        'id': uuid.uuid4().hex,
        'operations': {},
      }
    else:
      self.c = c

  def append(self,storage,op):
    if storage not in self.c['operations']:
      self.c['operations'][storage] = []
    self.c['operations'][storage].append(op)

  def close(self):
    for storage,_ in self.c['operations'].items():
      self.service_registry[storage].cscope_close(self)

  def barrier(self):
    for storage,operations in self.c['operations'].items():
      self.service_registry[storage].cscope_barrier(self._id, operations)

  # getters
  @property
  def _id(self):
    return self.c['id']

  def __repr__(self):
    return str(self.c)

  def to_json(self):
    return json.dumps(self.c, default=str)

  @staticmethod
  def from_json(service_registry, j):
    return Cscope(service_registry, c=json.loads(j))