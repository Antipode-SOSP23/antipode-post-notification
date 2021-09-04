import os
import pymysql
import pymysql.cursors
from datetime import datetime

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

def write_post(i,k):
  try:
    # connect to mysql
    mysql_conn = _mysql_connection('writer')
    op = (MYSQL_POST_TABLE_NAME, 'v', k)

    with mysql_conn.cursor() as cursor:
      # write with 0:AAAA -> blob of 1Mb
      # 1MB is the maximum packet size!!
      sql = f"INSERT INTO `{op[0]}` (`k`, `v`, `b`) VALUES (%s, %s, %s)"
      cursor.execute(sql, (int(i), op[2], os.urandom(1000000)))
      mysql_conn.commit()

    return op
  except pymysql.Error as e:
    print(f"[ERROR] MySQL exception writing post: {e}")
    exit(-1)

def read_post(k, evaluation):
  # connect to mysql
  mysql_conn = _mysql_connection('reader')

  # evaluation keys to fill
  # {
  #   'read_post_retries' : 0,
  #   'ts_read_post_spent': None,
  # }

  # read post
  ts_read_post_start = datetime.utcnow().timestamp()
  while True:
    with mysql_conn.cursor() as cursor:
      sql = f"SELECT `k` FROM `{MYSQL_POST_TABLE_NAME}` WHERE `v`=%s"
      cursor.execute(sql, (k,))
      result = cursor.fetchone()

      # current date and time
      if result is None:
        evaluation['read_post_retries'] += 1
        print(f"[RETRY] Read 'k' v='{k}'", flush=True)
      else:
        break

  while True:
    with mysql_conn.cursor() as cursor:
      sql = f"SELECT `b` FROM `{MYSQL_POST_TABLE_NAME}` WHERE `v`=%s"
      cursor.execute(sql, (k,))
      result = cursor.fetchone()

      # current date and time
      if result is None:
        evaluation['read_post_retries'] += 1
        print(f"[RETRY] Read 'b' v='{k}'", flush=True)
      else:
        evaluation['ts_read_post_spent_ms'] = int((datetime.utcnow().timestamp() - ts_read_post_start) * 1000)
        break

def antipode_bridge(id, role):
  import antipode_mysql as ant # this file will get copied when deploying

  return ant.AntipodeMysql(_id=id, conn=_mysql_connection(role))

def clean():
  None