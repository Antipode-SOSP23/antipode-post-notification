import json
import os
import boto3
from botocore.exceptions import ClientError
import pymysql
import pymysql.cursors
from pprint import pprint
from datetime import datetime


#---------------
# AWS SAM Deployment details
#
# Region: us-east-1 (N. Virginia)
# S3 Bucket: antipode-lambda-us
# Stack name: antipode-lambda-sql-only-reader
#
# Payload Example:
# { "i": "1", "key": "AABB11" }
#---------------

def lambda_handler(event, context):
  reply = {
    'role': 'reader',
    'key': event['i'],
    'value': event['key'],
    'writer_reply': {},
    'evaluation': {
      'ts_start': None,
      'ts_writer_reply': None,
      'read_notf_retries' : 0,
      'read_post_retries' : 0,
      'ts_read_notf_start': None,
      'ts_read_notf_exit': None,
      'ts_read_post_exit': None,
    }
  }

  reply['evaluation']['ts_start'] = datetime.now()

  writer_client = boto3.client('lambda', region_name='eu-central-1')
  response = writer_client.invoke(
    FunctionName='arn:aws:lambda:eu-central-1:641424397462:function:antipode-lambda-sql-only-antipodelambdasqlonlywri-1H8AZPL6KR9X5',
    InvocationType='RequestResponse',
    Payload=json.dumps(event),
  )
  body = json.loads(response['Payload'].read()).get('body', {})
  if not body:
    print("[ERROR] Empty reply!")
    return {
      'statusCode': 500
    }
  else:
    reply['evaluation']['ts_writer_reply'] = datetime.now()
    reply['writer_reply'] = body

    # open sql connection conne
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

    # read notification
    print("[INFO] Reading notification", end='')
    reply['evaluation']['ts_read_notf_start'] = datetime.now()
    while True:
      with mysql_conn.cursor() as cursor:
        print('', end ='...')
        sql = "SELECT `v` FROM `keyvalue` WHERE `k`=%s"
        cursor.execute(sql, (event['i'],))
        result = cursor.fetchone()

      # current date and time
      if result is None:
        break
      else:
        reply['evaluation']['read_notf_retries'] += 1

    reply['evaluation']['ts_read_notf_exit'] = datetime.now()
    print("[INFO] Done!")


    # read post
    print("[INFO] Reading post", end='')
    while True:
      with mysql_conn.cursor() as cursor:
        print('', end ='...')
        sql = "SELECT `b` FROM `blobs` WHERE `v`=%s"
        cursor.execute(sql, (event['key'],))
        result = cursor.fetchone()

      # current date and time
      if result is None:
        reply['evaluation']['read_post_retries'] += 1
        print("\t Retry read ...")
      else:
        break

    reply['evaluation']['ts_read_post_exit'] = datetime.now()
    print("[INFO] Done!")


    return {
      'statusCode': 200,
      'body': json.dumps(reply, default=str),
    }
