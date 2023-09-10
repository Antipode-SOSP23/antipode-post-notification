import json
import random

class Context:
  def __init__(self, c=None):
    if c is None:
      self.c = {
        'id': str(random.getrandbits(128)),
        'operations': {},
      }
    else:
      self.c = c

  # getters
  @property
  def _id(self):
    return self.c['id']

  def __repr__(self):
    return str(self.c)

  def to_json(self):
    return json.dumps(self.c, default=str)

  @staticmethod
  def from_json(j):
    return Context(json.loads(j))