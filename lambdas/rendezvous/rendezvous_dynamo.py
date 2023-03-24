import os
import botocore
from datetime import datetime, timedelta
from rendezvous_shim import RendezvousShim

DYNAMO_RENDEZVOUS_TABLE = os.environ['DYNAMO_RENDEZVOUS_TABLE']

class RendezvousDynamo(RendezvousShim):
  def __init__(self, conn, region):
    super().__init__(region)
    self.conn = conn
    self.rendezvous_table = self.conn.Table(DYNAMO_RENDEZVOUS_TABLE)
    self.last_evaluated_key = None

  def _parse_metadata(self, item):
    return item['rid'], item['service'], item['ts']

  def _read_metadata(self):
    try:
      if not self.last_evaluated_key:
        response = self.rendezvous_table.scan()
      else:
        response = self.rendezvous_table.scan(ExclusiveStartKey=self.last_evaluated_key)

      # dynamo can only scan up to 1 MB of data
      # track last evaluated key to continue scanning in the next function call and reduce amount of returned items
      self.last_evaluated_key = response.get('LastEvaluatedKey', None)
      
      return response['Items']
    
    except botocore.exceptions.BotoCoreError as e:
      print(f"[ERROR] DynamoDB exception reading rendezvous metadata: {e}", flush=True)

    return []