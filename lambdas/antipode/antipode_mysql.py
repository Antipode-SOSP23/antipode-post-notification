import os
import pymysql
import antipode as ant

MYSQL_ANTIPODE_TABLE = os.environ['MYSQL_ANTIPODE_TABLE']

class AntipodeMysql:
  def __init__(self, _id, conn):
    self._id = _id
    self.conn = conn

  def _id(self):
    return self._id

  def cscope_close(self, c):
    with self.conn.cursor() as cursor:
      sql = f"INSERT INTO `{MYSQL_ANTIPODE_TABLE}` (`cid`,`json`) VALUES (%s,%s)"
      cursor.execute(sql, (c._id,c.to_json()))
      self.conn.commit()

  def retrieve_cscope(self, cscope_id, service_registry):
    # read cscope_id
    while True:
      with self.conn.cursor() as cursor:
        sql = f"SELECT `json` FROM `{MYSQL_ANTIPODE_TABLE}` WHERE `cid`=%s"
        cursor.execute(sql, (cscope_id,))
        result = cursor.fetchone()
        if result is not None:
          return ant.Cscope.from_json(service_registry, result[0])

  def cscope_barrier(self, operations):
    # read post operations
    for op in operations:
      while True:
        with self.conn.cursor() as cursor:
          sql = f"SELECT 1 FROM `{op[0]}` WHERE `{op[1]}`=%s"
          cursor.execute(sql, (op[2],))
          if cursor.fetchone() is not None:
            break
