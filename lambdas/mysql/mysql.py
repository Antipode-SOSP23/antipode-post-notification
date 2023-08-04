import os
import pymysql
import pymysql.cursors

MYSQL_POST_TABLE_NAME = os.environ['MYSQL_POST_TABLE_NAME']

def _mysql_connection(role):
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

def write_post(k):
  try:
    # connect to mysql
    mysql_conn = _mysql_connection('writer')
    op = (k,)
    with mysql_conn.cursor() as cursor:
      # write with 0:AAAA -> blob of 1Mb
      # 1MB is the maximum packet size!!
      sql = f"INSERT INTO `{MYSQL_POST_TABLE_NAME}` (`k`, `b`) VALUES (%s, %s)"
      cursor.execute(sql, (k, os.urandom(1000000)))
      mysql_conn.commit()
    return op
  except pymysql.Error as e:
    print(f"[ERROR] MySQL exception writing post: {e}")
    exit(-1)

def read_post(k):
  # connect to mysql
  mysql_conn = _mysql_connection('reader')
  with mysql_conn.cursor() as cursor:
    sql = f"SELECT `b` FROM `{MYSQL_POST_TABLE_NAME}` WHERE `k`=%s"
    cursor.execute(sql, (k,))
    result = cursor.fetchone()
    # result is None if not found
    return not(result is None)

def clean():
  None