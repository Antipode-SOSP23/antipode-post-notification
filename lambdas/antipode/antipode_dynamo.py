import os
import boto3

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
        'cid': str(c._id),
      })

  def cscope_barrier(self, cscope_id, operations):
    # read cscope_id
    while True:
      if 'Item' in self.antipode_table.get_item(Key={'cid': str(cscope_id)}):
        break

    # read post operations
    for op in operations:
      op_table = self.conn.Table(op[0])
      while True:
        if 'Item' in op_table.get_item(Key={op[1]: str(op[2])}):
          break
