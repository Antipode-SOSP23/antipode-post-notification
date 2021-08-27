import json
import os
import boto3
import pymysql
import pymysql.cursors
from pprint import pprint
import time
from datetime import datetime

#---------------
# AWS SAM Deployment details
#
# Region: eu-central-1 (Frankfurt)
#
# Payload Example:
# { "i": "1", "key": "AABB11" }
#---------------

MYSQL_HOST = os.environ.get('MYSQL_HOST')
MYSQL_PORT = int(os.environ.get('MYSQL_PORT'))
MYSQL_USER = os.environ.get('MYSQL_USER')
MYSQL_PASSWORD = os.environ.get('MYSQL_PASSWORD')
MYSQL_DB = os.environ.get('MYSQL_DB')
MYSQL_POST_TABLE_NAME = os.environ.get('MYSQL_POST_TABLE_NAME')
SNS_ARN = os.environ.get('SNS_ARN')
ANTIPODE = bool(int(os.environ.get('ANTIPODE')))

def lambda_handler(event, context):
  try:
    # connect to mysql
    mysql_conn = pymysql.connect(
      host=MYSQL_HOST,
      port=MYSQL_PORT,
      user=MYSQL_USER,
      password=MYSQL_PASSWORD,
      db=MYSQL_DB,
      connect_timeout=30,
      autocommit=True
    )

    # write post
    with mysql_conn.cursor() as cursor:
      # write with 0:AAAA -> blob of 1Mb
      # 1MB is the maximum packet size!!
      sql = f"INSERT INTO `{MYSQL_POST_TABLE_NAME}` (`k`, `v`, `b`) VALUES (%s, %s, %s)"
      cursor.execute(sql, (int(event['i']), event['key'], os.urandom(1000000)))
      mysql_conn.commit()

    # returns OK with the k,v written
    event['written_at'] = str(datetime.utcnow())

    # write notification to SNS topic
    boto3.client('sns').publish(
      TargetArn=SNS_ARN,
      Message=json.dumps(event)
    )

    # return the event and the code
    return {
      'statusCode': 200,
      'body': json.dumps(event, default=str)
    }
  except pymysql.Error as e:
    print(f"[ERROR] MySQL exception: {e}")
    exit(-1)