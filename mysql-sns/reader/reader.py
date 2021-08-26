import json
import os
import pymysql
import pymysql.cursors
from pprint import pprint
from datetime import datetime
import boto3
import time


#---------------
# AWS SAM Deployment details
#
# Region: us-east-1 (N. Virginia)
# S3 Bucket: antipode-lambda-us
# Stack name: antipode-lambda-sns-reader
#
# Payload Example:
#   - do not forget to invoke the writer first
# { "i": "1", "key": "AABB11", "written_at": "2020-07-13 10:44:29.767250" }
#---------------

MYSQL_HOST = os.environ.get('MYSQL_HOST')
MYSQL_PORT = int(os.environ.get('MYSQL_PORT'))
MYSQL_USER = os.environ.get('MYSQL_USER')
MYSQL_PASSWORD = os.environ.get('MYSQL_PASSWORD')
MYSQL_DB = os.environ.get('MYSQL_DB')
MYSQL_POST_TABLE_NAME = os.environ.get('MYSQL_POST_TABLE_NAME')

def lambda_handler(event, context):
  # if we have an event from SNS topic we parse it
  # otherwise we already receiving an event through standard lambda API
  if 'Records' in event:
    event = json.loads(event['Records'][0]['Sns']['Message'])

  evaluation = {
    'i': event['i'],
    'ts_sns_spent': (datetime.now() - datetime.strptime(event['written_at'], '%Y-%m-%d %H:%M:%S.%f')).total_seconds(),
    'read_post_retries' : 0,
    'ts_read_post_spent': None,
    'read_post_key_retries' : 0,
    'ts_read_post_key_spent': None,
    'read_post_blob_retries' : 0,
    'ts_read_post_blob_spent': None,
  }

  # connect to mysql
  while True:
    try:
      mysql_conn = pymysql.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        db=MYSQL_DB,
        connect_timeout=30,
        autocommit=True
      )
      break
    except pymysql.Error as e:
      print(f"[ERROR] MySQL exception: {e}")

  # read post
  ts_read_post_key_start = datetime.now()
  while True:
    with mysql_conn.cursor() as cursor:
      sql = f"SELECT `k` FROM `{MYSQL_POST_TABLE_NAME}` WHERE `v`=%s"
      cursor.execute(sql, (event['key'],))
      result = cursor.fetchone()

      # current date and time
      if result is None:
        evaluation['read_post_retries'] += 1
        evaluation['read_post_key_retries'] += 1
        print("\t Retry read key...")
      else:
        evaluation['ts_read_post_key_spent'] = (datetime.now() - ts_read_post_key_start).total_seconds()
        break

  ts_read_post_blob_start = datetime.now()
  while True:
    with mysql_conn.cursor() as cursor:
      sql = f"SELECT `b` FROM `{MYSQL_POST_TABLE_NAME}` WHERE `v`=%s"
      cursor.execute(sql, (event['key'],))
      result = cursor.fetchone()

      # current date and time
      if result is None:
        evaluation['read_post_retries'] += 1
        evaluation['read_post_blob_retries'] += 1
      else:
        evaluation['ts_read_post_blob_spent'] = (datetime.now() - ts_read_post_blob_start).total_seconds()
        evaluation['ts_read_post_spent'] = (datetime.now() - ts_read_post_key_start).total_seconds()
        break

  # write evaluation to dynamo
  # table_conn = boto3.resource('dynamodb', region_name='eu-central-1').Table("antipode-eval")
  # table_conn.put_item(Item=evaluation)

  return {
    'statusCode': 200,
    'body': json.dumps(evaluation, default=str)
  }
