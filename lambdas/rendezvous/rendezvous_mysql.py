import os
from rendezvous_shim import RendezvousShim

MYSQL_RENDEZVOUS_TABLE = os.environ['MYSQL_RENDEZVOUS_TABLE']
RENDEZVOUS_ADDRESS = os.environ['RENDEZVOUS_ADDRESS']

class RendezvousMysql(RendezvousShim):
  def __init__(self, conn, service, region):
    super().__init__(service, region)
    self.conn = conn
    self.offset = 0
    self.max_records = 10000

  def find_metadata(self, bid):
    with self.conn.cursor() as cursor:
      sql = f"SELECT `bid` FROM `{MYSQL_RENDEZVOUS_TABLE}` WHERE `bid` = %s"
      cursor.execute(sql, (bid,))
      records = cursor.fetchall()

      if records:
        return records[0][0]
      
      self.inconsistency = True
      return None

  def _parse_metadata(self, record):
    return record
  
  def read_all_metadata(self):
    with self.conn.cursor() as cursor:
      # fetch non-expired metadata
      # is it worth ordering just to control an offset??
      sql = f"SELECT `bid` FROM `{MYSQL_RENDEZVOUS_TABLE}` WHERE `ts` >= DATE_SUB(NOW(), INTERVAL %s SECOND) ORDER BY `ts` LIMIT %s,%s"
      cursor.execute(sql, (self.metadata_validity_s, self.offset, self.max_records))
      records = cursor.fetchall()
      num_records = cursor.rowcount

      if cursor.rowcount == self.max_records:
        # track next offset to continue reading in the next function call
        self.offset += num_records
      else:
        # all records were read
        # reset offset to make sure we do not miss any 'old' records inserted in the meantime
        self.offset = 0
      return records
