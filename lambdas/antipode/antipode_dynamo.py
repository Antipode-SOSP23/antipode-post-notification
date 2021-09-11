import os
import boto3
import antipode as ant

DYNAMO_ANTIPODE_TABLE = os.environ['DYNAMO_ANTIPODE_TABLE']

class AntipodeDynamo:
  def __init__(self, _id, conn):
    self._id = _id
    self.conn = conn
    self.antipode_table = self.conn.Table(DYNAMO_ANTIPODE_TABLE)

  def _id(self):
    return self._id

  def cscope_close(self, c):
    # write post
    self.antipode_table.put_item(Item={
        # TODO: add FULL cscope
        'cid': str(c._id),
        'json': c.to_json(),
      })

  def retrieve_cscope(self, cscope_id, service_registry):
    # read cscope_id
    while True:
      item = self.antipode_table.get_item(Key={'cid': str(cscope_id)})
      if 'Item' in item:
        return ant.Cscope.from_json(service_registry, item['Item']['json'])

  def cscope_barrier(self, operations):
    # read post operations
    for op in operations:
      op_table = self.conn.Table(op[0])
      while True:
        if 'Item' in op_table.get_item(Key={op[1]: str(op[2])}, AttributesToGet=[op[1]]):
          break
