import os
import pymysql

MYSQL_ANTIPODE_TABLE = os.environ['MYSQL_ANTIPODE_TABLE']

class AntipodeMysql:
  def __init__(self, _id, conn):
    self._id = _id
    self.conn = conn

  def _id(self):
    return self._id

  def cscope_close(self, c):
    # write post
    with self.conn.cursor() as cursor:
      sql = f"INSERT INTO `{MYSQL_ANTIPODE_TABLE}` (`cid`) VALUES (%s)"
      cursor.execute(sql, (c._id))
      self.conn.commit()

  def cscope_barrier(self, cscope_id, operations):
    # read cscope_id
    while True:
      with self.conn.cursor() as cursor:
        sql = f"SELECT 1 FROM `{MYSQL_ANTIPODE_TABLE}` WHERE `cid`=%s"
        cursor.execute(sql, (cscope_id,))
        if cursor.fetchone() is not None:
          break

    # read post operations
    for op in operations:
      while True:
        with self.conn.cursor() as cursor:
          sql = f"SELECT 1 FROM `{op[0]}` WHERE `{op[1]}`=%s"
          cursor.execute(sql, (op[2],))
          if cursor.fetchone() is not None:
            break
