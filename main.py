import sys
import random
import string
import json
from pprint import pprint
import pymysql
import pymysql.cursors
import boto3
from boto3.dynamodb.types import TypeDeserializer
from datetime import datetime
import pandas as pd
import multiprocessing as mp
from multiprocessing import Pool
import time
import psutil

MAX_CONNECTIONS = 500
ITER = 5000
# ITER = 10

#############################
# CLEANERS
#
def _clean_mysql():
  # clean table before running lambda
  print("[INFO] Truncating MySQL table... ", end='')
  db = 'antipode'
  mysql_conn = pymysql.connect('antipode-lambda-global-cluster-1.cluster-citztxl8ztvl.eu-central-1.rds.amazonaws.com',
      port = 3306,
      user='antipode',
      password='antipode',
      connect_timeout=60,
      autocommit=True
    )
  with mysql_conn.cursor() as cursor:
    try:
      sql = f"DROP DATABASE `{db}`"
      cursor.execute(sql)
    except pymysql.err.InternalError as e:
      print(f"[WARN] MySQL error: {e}")

    try:
      sql = f"CREATE DATABASE `{db}`"
      cursor.execute(sql)
      sql = f"USE `{db}`"
      cursor.execute(sql)
    except pymysql.err.InternalError as e:
      print(f"[WARN] MySQL error: {e}")

    post_table = 'blobs'
    sql = f"CREATE TABLE `{post_table}` (k BIGINT, v VARCHAR(8), b LONGBLOB)"
    cursor.execute(sql)
    mysql_conn.commit()
    sql = f"SELECT COUNT(*) FROM `{post_table}`"
    cursor.execute(sql)
    assert(cursor.fetchone()[0] == 0)

    notf_table = 'keyvalue'
    sql = f"CREATE TABLE `{notf_table}` (k BIGINT, v VARCHAR(8))"
    cursor.execute(sql)
    mysql_conn.commit()
    sql = f"SELECT COUNT(*) FROM `{notf_table}`"
    cursor.execute(sql)
    assert(cursor.fetchone()[0] == 0)

  print("Done!")

def _clean_dynamo():
  print("[INFO] Truncating Dynamo table... ", end='')
  table_names = ['keyvalue', 'blobs', 'antipode-eval']

  for table_name in table_names:
    table = boto3.resource('dynamodb').Table(table_name)

    #get the table keys
    tableKeyNames = [ key.get("AttributeName") for key in table.key_schema ]
    keys = ", ".join(tableKeyNames)
    #Only retrieve the keys for each item in the table (minimize data transfer)
    response = table.scan(ProjectionExpression=keys)
    data = response.get('Items')

    while 'LastEvaluatedKey' in response:
      response = table.scan(ProjectionExpression, ExclusiveStartKey=response['LastEvaluatedKey'])
      data.extend(response['Items'])

    with table.batch_writer() as batch:
      for each in data:
        batch.delete_item(Key={key: each[key] for key in tableKeyNames})

    table_describe = boto3.client('dynamodb').describe_table(TableName=table_name)
    # assert(table_describe['Table']['ItemCount'] == 0)
  print("Done!")

def _clean_sqs():
  print("[INFO] Purging SQS queue... ", end='')
  sqs = boto3.resource('sqs', region_name='us-east-1')
  queue = sqs.get_queue_by_name(QueueName='antipode-eval')
  queue.purge()
  print("Done!")

#############################
# GATHERERS
#
def _gather_dynamo(data):
  print("[INFO] Parsing evaluation ...", end='')
  new_data = []
  for d in data:
    # skip entries that are empty
    if any([ d[f] is None for f in ['ts_read_notf_exit', 'ts_read_notf_start', 'ts_read_post_exit', 'ts_start', 'ts_writer_reply'] ]):
      continue

    # parse datetimes
    d['ts_read_notf_exit'] = datetime.strptime(d['ts_read_notf_exit'], '%Y-%m-%d %H:%M:%S.%f')
    d['ts_read_notf_start'] = datetime.strptime(d['ts_read_notf_start'], '%Y-%m-%d %H:%M:%S.%f')
    d['ts_read_post_exit'] = datetime.strptime(d['ts_read_post_exit'], '%Y-%m-%d %H:%M:%S.%f')
    d['ts_start'] = datetime.strptime(d['ts_start'], '%Y-%m-%d %H:%M:%S.%f')
    d['ts_writer_reply'] = datetime.strptime(d['ts_writer_reply'], '%Y-%m-%d %H:%M:%S.%f')

    # gather durations in seconds
    d['time_spent_reader'] = (d['ts_read_post_exit'] - d['ts_start']).total_seconds()
    d['time_spent_writer'] = (d['ts_writer_reply'] - d['ts_start']).total_seconds()
    d['time_spent_notf'] = (d['ts_read_notf_exit'] - d['ts_read_notf_start']).total_seconds()
    d['time_spent_post'] = (d['ts_read_post_exit'] - d['ts_read_notf_exit']).total_seconds()

    # delete timestamps
    del d['ts_read_notf_exit']
    del d['ts_read_notf_start']
    del d['ts_read_post_exit']
    del d['ts_start']
    del d['ts_writer_reply']

  df = pd.DataFrame(data)
  print("Done!")

  print(df.describe())

def _gather_sns():
  print("[INFO] Reading eval entries from Dynamo ...")
  table = boto3.resource('dynamodb').Table('antipode-eval')
  last_evaluated_key = None
  results = {}
  prev_len = 0
  while len(results) < ITER:
    print(f"\tCurrent results: {len(results)}")
    prev_len = len(results)
    response = table.scan()
    with table.batch_writer() as batch:
      for item in response.get('Items', []):
        if item['ts_read_post_spent'] is None or item['read_post_retries'] is None:
          ITER -= 1
          next
        # example entry:
        #   {'i': Decimal('7'), 'read_post_retries': Decimal('0'), 'ts_read_post_spent': Decimal('161')}
        results[int(item['i'])] = {
          'read_post_retries': int(item['read_post_retries']),
          'ts_read_post_spent': int(item['ts_read_post_spent']),
        }
    if prev_len == len(results):
      time.sleep(5)

  print("Done!")

  print("[INFO] Parsing evaluation ...", end='')
  df = pd.DataFrame(results.values())
  print("[INFO] Done!")

  print(df.describe())

def _gather_sns_with_sqs():
  print("[INFO] Waiting for all messages to arrive at eval queue ...")
  sqs = boto3.client('sqs', region_name='us-east-1')
  while True:
    reply = sqs.get_queue_attributes(
        QueueUrl='https://sqs.us-east-1.amazonaws.com/641424397462/antipode-eval',
        AttributeNames=[ 'ApproximateNumberOfMessages' ]
      )
    num_messages = int(reply['Attributes']['ApproximateNumberOfMessages'])
    print(f"\tPending {num_messages}/{ITER}")
    if num_messages >= ITER:
      break
    else:
      time.sleep((ITER-num_messages)/1000.0)
  print("[INFO] Done!")

  print("[INFO] Gater info through SQS...")
  sqs = boto3.resource('sqs', region_name='us-east-1')
  queue = sqs.get_queue_by_name(QueueName='antipode-eval')

  results = {}
  while len(results) < ITER:
    messages = queue.receive_messages(MaxNumberOfMessages=10)
    print(f"\tRead {len(messages)} messages:")
    if len(messages) == 0:
      sleep(10)
    for message in messages:
      # example entry:
      #   {'i': Decimal('7'), 'read_post_retries': Decimal('0'), 'ts_read_post_spent': Decimal('161')}
      item = json.loads(json.loads(message.body)['responsePayload']['body'])
      results[int(item['i'])] = {
        'read_post_retries': int(item['read_post_retries']),
        'ts_read_post_spent': int(item['ts_read_post_spent']),
      }

      print(f"\t\tReading #{item['i']} - {len(results)}/{ITER} ...")
      message.delete()
  print("Done!")

  print("[INFO] Parsing evaluation ...", end='')
  df = pd.DataFrame(results.values())
  print("[INFO] Done!")

  print(df.describe())


#############################
# DYNAMO ONLY
#
def start_dynamo():
  _clean_dynamo()
  _clean_mysql()

  print("[INFO] Running...", end='')
  manager = mp.Manager()
  evaluation = manager.dict()
  # created pool running maximum 4 tasks
  pool = mp.Pool(4)
  for i in range(ITER):
    pool.apply_async(_lambda_reader_invoke, args=(evaluation, i))
  pool.close()
  pool.join()
  _gather_dynamo(dict(evaluation).values())
  print("[INFO] Done!")

#############################
# SQL ONLY
#
def _lambda_sql_only_invoke(evaluation,i):
  event_payload = json.dumps({
    "i": i,
    "key": ''.join(random.choices(string.ascii_uppercase + string.digits, k=6)),
  })

  # call reader
  print(f"\tRunning {i}...", end='')
  while True:
    try:
      reader_client = boto3.client('lambda', region_name='us-east-1')
      response = reader_client.invoke(
          FunctionName='arn:aws:lambda:us-east-1:641424397462:function:antipode-lambda-sql-only-antipodelambdasqlonlyrea-1NIXL0J15WX62',
          InvocationType='RequestResponse',
          Payload=event_payload,
        )
      payload = json.loads(response['Payload'].read())
      code = int(payload.get('statusCode', 500))
      if code == 200:
        evaluation[i] = json.loads(payload['body'])['evaluation']
        print(f"Done!")
        break
      else:
        print(f"[ERROR] AWS Lambda did not return 200: {payload}")
      time.sleep(1)
    except Exception as e:
      request_id = response['ResponseMetadata']['RequestId']
      print(f"[ERROR] Excep1tion while invoking AWS Writer Lambda for #{i} and request id '{request_id}':\n\t{e}")
      time.sleep(5)
    finally:
      sys.stdout.flush()

def start_sql_only():
  _clean_mysql()

  print("[INFO] Running... ")
  manager = mp.Manager()
  evaluation = manager.dict()
  for r in range(max(1, int(ITER/MAX_CONNECTIONS))):
    print(f"\t--- Round #{r} --")

    pool = mp.Pool(psutil.cpu_count())
    for i in range(MAX_CONNECTIONS*r, MAX_CONNECTIONS*r + MAX_CONNECTIONS):
      pool.apply_async(_lambda_sql_only_invoke, args=(evaluation, i))
    pool.close()
    print("\tWaiting on tasks... ")
    pool.join()
    print("\tDone... ")

  print("[INFO] Done!")

  _gather_dynamo(dict(evaluation).values())

#############################
# DYNAMO PLUS SQL
#
def _lambda_dynamo_plus_sql_invoke(evaluation,i):
  event_payload = json.dumps({
    "i": i,
    "key": ''.join(random.choices(string.ascii_uppercase + string.digits, k=6)),
  })

  # call reader
  print(f"\tRunning {i}...", end='')
  while True:
    try:
      reader_client = boto3.client('lambda', region_name='us-east-1')
      response = reader_client.invoke(
          FunctionName='arn:aws:lambda:us-east-1:641424397462:function:antipode-lambda-dyplussql-antipodelambdadyplussqlr-165OMBK52NYJZ',
          InvocationType='RequestResponse',
          Payload=event_payload,
        )
      payload = json.loads(response['Payload'].read())
      code = int(payload.get('statusCode', 500))
      if code == 200:
        evaluation[i] = json.loads(payload['body'])['evaluation']
        print(f"Done!")
        break
      else:
        print(f"[ERROR] AWS Lambda did not return 200: {payload}")
      time.sleep(1)
    except Exception as e:
      request_id = response['ResponseMetadata']['RequestId']
      print(f"[ERROR] Excep1tion while invoking AWS Writer Lambda for #{i} and request id '{request_id}':\n\t{e}")
      time.sleep(5)
    finally:
      sys.stdout.flush()

def start_dynamo_plus_sql_only():
  _clean_mysql()

  print("[INFO] Running... ")
  manager = mp.Manager()
  evaluation = manager.dict()
  for r in range(max(1, int(ITER/MAX_CONNECTIONS))):
    print(f"\t--- Round #{r} --")

    pool = mp.Pool(psutil.cpu_count())
    for i in range(MAX_CONNECTIONS*r, MAX_CONNECTIONS*r + MAX_CONNECTIONS):
      pool.apply_async(_lambda_dynamo_plus_sql_invoke, args=(evaluation, i))
    pool.close()
    print("\tWaiting on tasks... ")
    pool.join()
    print("\tDone... ")

  print("[INFO] Done!")

  _gather_dynamo(dict(evaluation).values())


#############################
# SNS
#
def _lambda_sns_invoke(i):
  payload = {
    "i": i,
    "key": ''.join(random.choices(string.ascii_uppercase + string.digits, k=6)),
  }

  print(f"\tRunning {i}...", end='')
  # call reader
  while True:
    try:
      writer_client = boto3.client('lambda', region_name='eu-central-1')
      response = writer_client.invoke(
        FunctionName='arn:aws:lambda:eu-central-1:641424397462:function:antipode-lambda-sns-writer-antipodelambdasnswriter-KF1XUHS9ULYR',
        InvocationType='RequestResponse',
        Payload=json.dumps(payload),
      )
      code = int(json.loads(response['Payload'].read()).get('statusCode', 500))
      if code == 200:
        print(f"Done!")
        break

      sys.stdout.flush()
    except Exception as e:
      print(f"[ERROR] Exception while invoking AWS Writer Lambda: {e}")
      pass

def start_sns():
  _clean_mysql()
  _clean_dynamo()
  _clean_sqs()

  print("[INFO] Running... ")
  for r in range(max(1, int(ITER/MAX_CONNECTIONS))):
    print(f"\t--- Round #{r} --")
    # for i in range(ITER):
    #   _lambda_sns_invoke(i)

    pool = mp.Pool(processes=psutil.cpu_count())
    pool.map(_lambda_sns_invoke, range(MAX_CONNECTIONS*r, MAX_CONNECTIONS*r + MAX_CONNECTIONS))
    pool.close()
    pool.join()

  print("[INFO] Done!")

  _gather_sns_with_sqs()

#############################
# MAIN
#
if __name__ == '__main__':
  start_dynamo_plus_sql_only()

# without fetch time - fetch B
# count         2000.00000         2000.000000
# mean             0.00250       246756.477500
# std              0.04995       149623.961279
# min              0.00000          444.000000
# 25%              0.00000       126371.750000
# 50%              0.00000       247845.000000
# 75%              0.00000       376600.000000
# max              1.00000       484361.000000

# with fetch time - fetch K
# count        1000.000000         1000.000000
# mean           40.452000          163.517000
# std            42.621101          173.906674
# min             0.000000            0.000000
# 25%             0.000000            0.000000
# 50%            29.000000          108.500000
# 75%            69.250000          285.250000
# max           201.000000          815.000000

# without fetch time - fetch K
# count        1000.000000         1000.000000
# mean           57.581000          242.200000
# std            66.927833          296.663795
# min             0.000000            0.000000
# 25%            15.000000           61.000000
# 50%            42.000000          169.000000
# 75%            74.000000          298.500000
# max           416.000000         1705.000000

# swith while with with on mysql conn
# count        1000.000000         1000.000000
# mean           34.594000          135.808000
# std            37.227047          146.169301
# min             0.000000            0.000000
# 25%             0.000000            4.000000
# 50%            25.000000           96.000000
# 75%            55.000000          214.000000
# max           185.000000          743.000000


# 10k - SNS
#        read_post_retries  ts_read_post_spent
# count        10000.00000        10000.000000
# mean            27.59600          201.007900
# std             31.17438          195.334813
# min              0.00000            0.000000
# 25%              2.00000           23.000000
# 50%             17.00000          162.000000
# 75%             43.00000          316.000000
# max            357.00000         1592.000000

# 10k - dynamo only
#        read_notf_retries  read_post_retries  time_spent_reader  time_spent_writer  time_spent_notf  time_spent_post
# count       10000.000000       10000.000000       10000.000000       10000.000000     10000.000000     10000.000000
# mean           11.001300          14.652800           2.237202           0.828056         0.572797         0.791970
# std             7.297727          11.973136           0.460329           0.078166         0.310182         0.517043
# min             0.000000           0.000000           1.119925           0.645902         0.049530         0.089410
# 25%             5.000000           4.000000           1.900966           0.794139         0.300582         0.340516
# 50%            11.000000          13.000000           2.192128           0.815482         0.577556         0.739543
# 75%            17.000000          23.000000           2.555330           0.841857         0.837704         1.142574
# max            30.000000          89.000000           5.798233           2.667064         1.438102         4.111954

# 2k - sql only
#        read_notf_retries  read_post_retries  time_spent_reader  time_spent_writer  time_spent_notf  time_spent_post
# count             1988.0        1988.000000        1988.000000        1988.000000      1988.000000      1988.000000
# mean                 0.0           4.537223           7.329634           1.472932         0.004282         5.808273
# std                  0.0          15.848716           4.394447           0.215774         0.011736         4.399615
# min                  0.0           0.000000           1.816680           1.314229         0.000628         0.223146
# 25%                  0.0           1.000000           3.789205           1.383450         0.001405         2.280057
# 50%                  0.0           1.000000           5.885877           1.409685         0.001839         4.317258
# 75%                  0.0           1.000000          10.082295           1.454273         0.002389         8.481750
# max                  0.0         156.000000          25.579203           2.903781         0.259202        24.062378

# 5k - sql only
#        read_notf_retries  read_post_retries  time_spent_reader  time_spent_writer  time_spent_notf  time_spent_post
# count             4899.0        4899.000000        4899.000000        4899.000000      4899.000000      4899.000000
# mean                 0.0           2.959788          18.064645           1.501048         0.017527        16.484854
# std                  0.0          12.054483          12.398301           0.307866         0.034965        12.365029
# min                  0.0           0.000000           1.982766           1.184953         0.000533         0.494479
# 25%                  0.0           1.000000           7.916324           1.380827         0.002115         6.353764
# 50%                  0.0           1.000000          14.650672           1.403381         0.003874        13.062231
# 75%                  0.0           1.000000          26.820272           1.441151         0.012459        25.249679
# max                  0.0         158.000000          59.768765           3.398093         0.463431        58.190985