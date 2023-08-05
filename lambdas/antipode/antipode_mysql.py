import os
import pymysql
import pymysql.cursors

MYSQL_ANTIPODE_TABLE = os.environ['MYSQL_ANTIPODE_TABLE_NAME']

def _conn(role):
  # connect to mysql
  role = role.upper()
  region = os.environ[f"{role}_REGION"].replace('-','_').upper()
  while True:
    try:
      return pymysql.connect(
          host=os.environ[f"MYSQL_HOST__{region}__{role}"],
          port=int(os.environ['MYSQL_PORT']),
          user=os.environ['MYSQL_USER'],
          password=os.environ['MYSQL_PASSWORD'],
          db=os.environ['MYSQL_DB'],
          connect_timeout=30,
          autocommit=True
        )
    except pymysql.Error as e:
      print(f"[ERROR] MySQL exception opening connection: {e}")

def write_post(k, c):
  try:
    mysql_conn = _conn('writer')
    with mysql_conn.cursor() as cursor:
      # write with 0:AAAA -> blob of 1Mb
      # 1MB is the maximum packet size!!
      sql = f"INSERT INTO `{MYSQL_ANTIPODE_TABLE}` (`k`, `b`, `c`) VALUES (%s, %s, %s)"
      cursor.execute(sql, (k, os.urandom(1000000), str(c._id)))
      mysql_conn.commit()
    wid = (k,)
    return wid
  except pymysql.Error as e:
    print(f"[ERROR] MySQL exception writing post: {e}")
    exit(-1)

def wait(operations):
  mysql_conn = _conn('reader')
  for (k,cid) in operations:
    while True:
      with mysql_conn.cursor() as cursor:
        sql = f"SELECT 1 FROM `{MYSQL_ANTIPODE_TABLE}` WHERE `c`=%s"
        cursor.execute(sql, (cid,))
        if cursor.fetchone() is not None:
          break

def read_post(k, c):
  # connect to mysql
  mysql_conn = _conn('reader')
  with mysql_conn.cursor() as cursor:
    sql = f"SELECT `b` FROM `{MYSQL_ANTIPODE_TABLE}` WHERE `k`=%s"
    cursor.execute(sql, (k,))
    result = cursor.fetchone()
    # result is None if not found
    return not(result is None)

def clean():
  None