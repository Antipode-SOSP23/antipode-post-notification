import os
from rendezvous_shim import RendezvousShim
from boto3.dynamodb.conditions import Key, Attr
import datetime
import time

DYNAMO_RENDEZVOUS_TABLE = os.environ['DYNAMO_RENDEZVOUS_TABLE']
DYNAMO_POST_TABLE_NAME = os.environ['DYNAMO_POST_TABLE_NAME']

class RendezvousDynamo(RendezvousShim):
  def __init__(self, conn, service, region):
    super().__init__(service, region)
    self.conn = conn
    self.rendezvous_table = self.conn.Table(DYNAMO_RENDEZVOUS_TABLE)
    self.post_table = self.conn.Table(DYNAMO_POST_TABLE_NAME)
    self.last_evaluated_key = None

  def _find_object(self, bid, obj_key):
    response = self.post_table.get_item(Key={'k': obj_key}, AttributesToGet=['rdv_bid'])
    if 'Item' in response and bid == response['Item']['rdv_bid']:
      return True
    return False

  def find_metadata(self, bid):
    response = self.rendezvous_table.get_item(Key={'bid': bid})
    if 'Item' in response:
      item = response['Item']

      # wait until object (post) is available
      if not self._find_object(bid, item['obj_key']):
        return False

      return True
    
    return False

  def _parse_metadata(self, item):
    return item['bid']

  def read_all_metadata(self):
    print("Reading all metadata...")
    time_ago = int(time.time()) - self.metadata_validity_s

    items = []
    if not self.last_evaluated_key:
      response = self.rendezvous_table.scan(FilterExpression=Attr("ts").gte(time_ago))
    else:
      response = self.rendezvous_table.scan(FilterExpression=Attr("ts").gte(time_ago), ExclusiveStartKey=self.last_evaluated_key)

    print("Got response: ", response)

    # track last evaluated key to continue scanning in the next function call
    self.last_evaluated_key = response.get('LastEvaluatedKey', None)
    
    for item in response.get('Items', []):
      # check if object (post) is available
      if self._find_object(item['bid'], item['obj_key']):
        items.append(item)

    return items