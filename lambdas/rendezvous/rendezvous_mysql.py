import os
import pymysql
import pymysql.cursors

MYSQL_POST_TABLE_NAME = os.environ['MYSQL_POST_TABLE_NAME']

def _conn(role):
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

def write_post(k, m):
  try:
    mysql_conn = _conn('writer')
    with mysql_conn.cursor() as cursor:
      sql = f"INSERT INTO `{MYSQL_POST_TABLE_NAME}` (`k`, `b`, `rdv_bid`) VALUES (%s, %s, %s)"
      cursor.execute(sql, (k, os.urandom(1000000), m))
      mysql_conn.commit()
  except pymysql.Error as e:
    print(f"[ERROR] MySQL exception writing post: {e}")
    exit(-1)