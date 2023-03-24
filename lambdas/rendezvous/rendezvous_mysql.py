import os
import pymysql.cursors
import pymysql
from datetime import datetime
from rendezvous_shim import RendezvousShim

MYSQL_RENDEZVOUS_TABLE_NAME = os.environ['MYSQL_RENDEZVOUS_TABLE_NAME']
RENDEZVOUS_ADDRESS = os.environ['RENDEZVOUS_ADDRESS']

class RendezvousMysql(RendezvousShim):
  def __init__(self, conn, region):
    super().__init__(region)
    self.conn = conn
    self.offset = 0
    self.max_records = 10000

  def _parse_time(self, ts):
    # time in sql is stored with second precision
    return datetime.strptime(str(ts), '%Y-%m-%d %H:%M:%S')
    
  def _parse_metadata(self, record):
    # in a mysql record, metadata is a list of values so we return the list itself
    return record

  
  def _read_metadata(self):
    #TODO define a trigger in aws mysql to delete expired records so we don't need to filter by timestamp. idk how to do it :(

    try:
      with self.conn.cursor() as cursor:
        # fetch metadata registered no more than 60 minutes ago
        sql = f"SELECT `rid`, `service`, `ts` FROM `{MYSQL_RENDEZVOUS_TABLE_NAME}` WHERE `ts` >= DATE_SUB(NOW(), INTERVAL 60 MINUTE) ORDER BY `ts` LIMIT %s,%s"
        cursor.execute(sql, (self.offset, self.max_records))
        records = cursor.fetchall()

        num_records = cursor.rowcount
        if cursor.rowcount == self.max_records:
          # track next offset to continue selecting in the next function call
          self.offset += num_records
        else:
          # reset offset if we have read all records
          # make sure we do not miss any 'old' records inserted in the meantime
          self.offset = 0
        return records
      
    except pymysql.Error as e:
      print(f"[ERROR] MySQL exception reading rendezvous metadata: {e}", flush=True)
      
    return []
