import os
from rendezvous_shim import RendezvousShim

DYNAMO_RENDEZVOUS_TABLE = os.environ['DYNAMO_RENDEZVOUS_TABLE']
DYNAMO_POST_TABLE_NAME = os.environ['DYNAMO_POST_TABLE_NAME']

class RendezvousDynamo(RendezvousShim):
  def __init__(self, conn, region):
    super().__init__(region)
    self.conn = conn
    self.rendezvous_table = self.conn.Table(DYNAMO_RENDEZVOUS_TABLE)
    self.post_table = self.conn.Table(DYNAMO_POST_TABLE_NAME)
    self.last_evaluated_key = None

  def _find_object(self, rid, obj_key):
    response = self.post_table.get_item(Key={'k': obj_key}, AttributesToGet=['rendezvous'])
    if 'Item' in response and rid in response['Item']['rendezvous']:
      return True
    return False


# ----------------
# Current request
# ----------------

  def read_metadata(self, rid):
    while True:
      response = self.rendezvous_table.get_item(Key={'rid': rid})
      if 'Item' in response:
        item = response['Item']

        # wait until object (post) is available
        while not self._find_object(item['rid'], item['obj_key']):
          self.inconsistency = True

        return item['bid']
      
      self.inconsistency = True

# -------------
# All requests
# -------------

  def _parse_metadata(self, item):
    return item['rid'], item['bid']

  def read_all_metadata(self):
    items = []
    if not self.last_evaluated_key:
      response = self.rendezvous_table.scan()
    else:
      response = self.rendezvous_table.scan(ExclusiveStartKey=self.last_evaluated_key)

    # track last evaluated key to continue scanning in the next function call
    self.last_evaluated_key = response.get('LastEvaluatedKey', None)
    
    for item in response.get('Items', []):
      # check if object (post) is available
      if item['rid'] not in self.metadata and self._find_object(item['rid'], item['obj_key']):
        items.append(item)

    return items