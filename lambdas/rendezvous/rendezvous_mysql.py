import os
from rendezvous_shim import RendezvousShim

MYSQL_RENDEZVOUS_TABLE = os.environ['MYSQL_RENDEZVOUS_TABLE']
RENDEZVOUS_ADDRESS = os.environ['RENDEZVOUS_ADDRESS']

class RendezvousMysql(RendezvousShim):
  def __init__(self, conn, region):
    super().__init__(region)
    self.conn = conn
    self.offset = 0
    self.max_records = 10000
    
# ----------------
# Current request
# ----------------

  def read_metadata(self, rid):
    while True:
      with self.conn.cursor() as cursor:
        sql = f"SELECT `bid` FROM `{MYSQL_RENDEZVOUS_TABLE}` WHERE `rid` = %s AND `ttl` > NOW()"
        cursor.execute(sql, (rid,))
        records = cursor.fetchall()

        if records:
          return records[0][0]
        
        self.inconsistency = True


# -------------
# All requests
# -------------

  def _parse_metadata(self, record):
    return record
  
  def read_all_metadata(self):
    with self.conn.cursor() as cursor:
      # fetch non-expired metadata
      sql = f"SELECT `rid`, `bid` FROM `{MYSQL_RENDEZVOUS_TABLE}` WHERE `ttl` > NOW() ORDER BY `ttl` LIMIT %s,%s"
      cursor.execute(sql, (self.offset, self.max_records))
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
