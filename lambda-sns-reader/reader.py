import json
import os
import boto3
import pymysql
import pymysql.cursors
from pprint import pprint
from datetime import datetime
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

  while True:
    try:
      mysql_conn = pymysql.connect('antipode-lambda-global-cluster-1.cluster-ro-cdwqaw2esdz0.us-east-1.rds.amazonaws.com',
          port = 3306,
          user='antipode',
          password='antipode',
          connect_timeout=5,
          db='antipode',
          autocommit=True
        )
      break
    except pymysql.Error as e:
      print(f"[ERROR] MySQL exception: {e}")

  # read post
  print("[INFO] Reading post", end='')

  ts_read_post_key_start = datetime.now()
  while True:
    with mysql_conn.cursor() as cursor:
      print('', end ='...')
      sql = "SELECT `k` FROM `blobs` WHERE `v`=%s"
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
      print('', end ='...')
      sql = "SELECT `b` FROM `blobs` WHERE `v`=%s"
      cursor.execute(sql, (event['key'],))
      result = cursor.fetchone()

      # current date and time
      if result is None:
        evaluation['read_post_retries'] += 1
        evaluation['read_post_blob_retries'] += 1
        print("\t Retry read post...")
      else:
        evaluation['ts_read_post_blob_spent'] = (datetime.now() - ts_read_post_blob_start).total_seconds()
        evaluation['ts_read_post_spent'] = (datetime.now() - ts_read_post_key_start).total_seconds()
        break

  print("[INFO] Done!")

  # write evaluation to dynamo
  # print("[INFO] Writing eval to dynamo...", end='')
  # table_conn = boto3.resource('dynamodb', region_name='eu-central-1').Table("antipode-eval")
  # table_conn.put_item(Item=evaluation)
  # print("[INFO] Done!")

  return {
    'statusCode': 200,
    'body': json.dumps(evaluation, default=str)
  }
