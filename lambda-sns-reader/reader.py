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
#---------------

def lambda_handler(event, context):
  event = json.loads(event['Records'][0]['Sns']['Message'])
  evaluation = {
    'i': event['i'],
    'read_post_retries' : 0,
    'ts_read_post_spent': None,
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
  ts_read_post_start = datetime.now()
  while True:
    with mysql_conn.cursor() as cursor:
      print('', end ='...')
      fetch_start = datetime.now()
      sql = "SELECT `k` FROM `keyvalue` WHERE `v`=%s"
      cursor.execute(sql, (event['key'],))
      result = cursor.fetchone()
      fetch_spent = int((datetime.now() - fetch_start).total_seconds() * 1000)

    # current date and time
    if result is None:
      evaluation['read_post_retries'] += 1
      print("\t Retry read ...")
    else:
      evaluation['ts_read_post_spent'] = int((datetime.now() - ts_read_post_start).total_seconds() * 1000)
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
