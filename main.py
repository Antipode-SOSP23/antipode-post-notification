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

ITER = 10000

#############################
# CLEANERS
#
def _clean_mysql():
  # clean table before running lambda
  print("[INFO] Truncating MySQL table... ", end='')
  table = 'keyvalue'
  mysql_conn = pymysql.connect('antipode-dporto-cluster-1.cluster-citztxl8ztvl.eu-central-1.rds.amazonaws.com',
      port = 3306,
      user='admin',
      password='adminadmin',
      connect_timeout=5,
      db='antipode',
      autocommit=True
    )
  with mysql_conn.cursor() as cursor:
    sql = f"DROP TABLE `{table}`"
    cursor.execute(sql)
    sql = f"CREATE TABLE `{table}` (k BIGINT, v VARCHAR(8), b LONGBLOB)"
    cursor.execute(sql)
    mysql_conn.commit()
    sql = f"SELECT COUNT(*) FROM `{table}`"
    cursor.execute(sql)
    assert(cursor.fetchone()[0] == 0)
  print("Done!")

def _clean_dynamo():
  print("[INFO] Truncating Dynamo table... ", end='')
  table_names = ['keyvalue', 'antipode-eval']

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

#############################
# DYNAMO
#
def _lambda_reader_invoke(evaluation,i):
  payload = {
    "i": i,
    "key": ''.join(random.choices(string.ascii_uppercase + string.digits, k=6)),
  }

  # call reader
  reader_client = boto3.client('lambda', region_name='us-east-1')
  response = reader_client.invoke(
    FunctionName='arn:aws:lambda:us-east-1:641424397462:function:antipode-lambda-reader-antipodelambdareader-1TUBTXAK5Z3H2',
    InvocationType='RequestResponse',
    Payload=json.dumps(payload),
  )

  # call writer
  # writer_client = boto3.client('lambda', region_name='eu-central-1')
  # response = client.invoke(
  #   FunctionName='arn:aws:lambda:eu-central-1:641424397462:function:antipode-lambda-writer-antipodelambdawriter-1HU9P3YKANACH',
  #   InvocationType='RequestResponse',
  #   Payload=json.dumps(payload),
  # )

  payload = json.loads(response['Payload'].read())
  body = json.loads(payload.get('body',"{}"))
  if not body:
    print("[ERROR] Empty reply!: ")
    pprint(payload)
    exit()
  else:
    evaluation[i] = body['evaluation']
    print(str(i), end='...')

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

def _gather_dynamo(data):
  # print("[INFO] Raw evaluation data:")
  # pprint(data)

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

#############################
# SNS
#
def _lambda_sns_invoke(i):
  payload = {
    "i": i,
    "key": ''.join(random.choices(string.ascii_uppercase + string.digits, k=6)),
  }

  writer_client = boto3.client('lambda', region_name='eu-central-1')
  response = writer_client.invoke(
    FunctionName='arn:aws:lambda:eu-central-1:641424397462:function:antipode-lambda-sns-writer-antipodelambdasnswriter-FWBNEBJ1XTTL',
    InvocationType='Event',
    Payload=json.dumps(payload),
  )

def start_sns():
  _clean_mysql()
  _clean_dynamo()

  print("[INFO] Running... ", end='')
  for i in range(ITER):
    _lambda_sns_invoke(i)
  print("Done!")

  print("[INFO] Reading eval to dynamo... ", end='')
  # evaluation = _gather_sns()
  print("Done!")

def _gather_sns():
  print("[INFO] Reading eval entries from Dynamo ...", end='')
  table = boto3.resource('dynamodb').Table('antipode-eval')
  last_evaluated_key = None
  results = {}
  while len(results) < ITER:
    response = table.scan()
    with table.batch_writer() as batch:
      for item in response.get('Items', []):
        # example entry:
        #   {'i': Decimal('7'), 'read_post_retries': Decimal('0'), 'ts_read_post_spent': Decimal('161')}
        results[int(item['i'])] = {
          'read_post_retries': int(item['read_post_retries']),
          'ts_read_post_spent': int(item['ts_read_post_spent']),
        }

  print("Done!")

  print("[INFO] Parsing evaluation ...", end='')
  df = pd.DataFrame(results.values())
  print("Done!")

  print(df.describe())

#############################
# MAIN
#
if __name__ == '__main__':
  start_sns()