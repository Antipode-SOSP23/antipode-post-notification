import uuid
import json
from collections import defaultdict

class AntipodeMysql:
  def __init__(self, _id, host, port, user, password, db):
    self._id = _id
    self.mysql_conn = pymysql.connect(
      host=host,
      port=port,
      user=user,
      password=password,
      db=db,
      connect_timeout=30,
      autocommit=True
    )

  def init(self):
    try:
      with self.mysql_conn.cursor() as cursor:
        sql = f"CREATE TABLE `cscopes` (cid VARCHAR(32))"
        cursor.execute(sql)
        self.mysql_conn.commit()
        sql = f"SELECT COUNT(*) FROM `cscopes`"
        cursor.execute(sql)
        assert(cursor.fetchone()[0] == 0)
    except (pymysql.err.OperationalError) as e:
      code, msg = e.args
      if code == 1050:
        # table already exists
        pass
      else:
        print(f"[WARN] MySQL error: {e}")
        exit(-1)

  def reset(self):
    with mysql_conn.cursor() as cursor:
      try:
        sql = f"DROP DATABASE `cscopes`"
        cursor.execute(sql)
      except (pymysql.err.InternalError, pymysql.err.OperationalError) as e:
        code, msg = e.args
        if code == 1008:
          # database does not exist hence we ignore
          pass
        else:
          print(f"[WARN] MySQL error: {e}")
          exit(-1)

  def _id(self):
    return self._id

  def cscope_close(self, c):
    # write post
    with self.mysql_conn.cursor() as cursor:
      sql = f"INSERT INTO `cscopes` (`cid`) VALUES (%s)"
      cursor.execute(sql, (c._id))
      self.mysql_conn.commit()

  def cscope_barrier(self, operations):
    while True:
      with self.mysql_conn.cursor() as cursor:
        sql = f"SELECT 1 FROM `cscopes` WHERE `cid`=%s"
        cursor.execute(sql, (c._id,))
        if cursor.fetchone() is not None:
          break

    # read post
    for op in operations:
      while True:
        with self.mysql_conn.cursor() as cursor:
          sql = f"SELECT 1 FROM `{op[0]}` WHERE `{op[1]}`=%s"
          cursor.execute(sql, (op[2],))
          if cursor.fetchone() is not None:
            break

class Cscope:
  def __init__(self, c=None):
    if c is None:
      self.c = {
        'id': uuid.uuid4().hex,
        'operations': {},
      }
    else:
      self.c = c

  def append(self,storage,op):
    if storage not in self.c['operations']:
      self.c['operations'][storage] = []
    self.c['operations'][storage].append(op)

  def close(self):
    for storage,_ in self.c['operations'].items():
      SERVICE_REGISTRY[storage].cscope_close(self)

  def barrier(self):
    for storage,operations in self.c['operations'].items():
      SERVICE_REGISTRY[storage].cscope_barrier(operations)

  # getters
  @property
  def _id(self):
    return self.c['id']

  def __repr__(self):
    return str(self.c)

  def to_json(self):
    return json.dumps(self.c, default=str)

  @staticmethod
  def from_json(j):
    return Cscope(c=json.loads(j))


SERVICE_REGISTRY = {
  'PostStorage': AntipodeMysql(_id='PostStorage',
      host=MYSQL_HOST,
      port=MYSQL_PORT,
      user=MYSQL_USER,
      password=MYSQL_PASSWORD,
      db=MYSQL_DB
    )
}