import pymysql

class AntipodeMysql:
  def __init__(self, _id, conn):
    self._id = _id
    self.conn = conn

  def _id(self):
    return self._id

  def cscope_close(self, c):
    # write post
    with self.conn.cursor() as cursor:
      sql = f"INSERT INTO `cscopes` (`cid`) VALUES (%s)"
      cursor.execute(sql, (c._id))
      self.conn.commit()

  def cscope_barrier(self, cscope_id, operations):
    while True:
      with self.conn.cursor() as cursor:
        sql = f"SELECT 1 FROM `cscopes` WHERE `cid`=%s"
        cursor.execute(sql, (cscope_id,))
        if cursor.fetchone() is not None:
          break

    # read post
    for op in operations:
      while True:
        with self.conn.cursor() as cursor:
          sql = f"SELECT 1 FROM `{op[0]}` WHERE `{op[1]}`=%s"
          cursor.execute(sql, (op[2],))
          if cursor.fetchone() is not None:
            break
