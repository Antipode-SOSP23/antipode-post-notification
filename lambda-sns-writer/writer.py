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
# S3 Bucket: antipode-lambda-eu
# Stack name: antipode-lambda-sns-writer
#
# Payload Example:
# { "i": "1", "key": "AABB11" }
#---------------

try:
  mysql_conn = pymysql.connect('antipode-lambda-global-cluster-1.cluster-citztxl8ztvl.eu-central-1.rds.amazonaws.com',
      port = 3306,
      user='antipode',
      password='antipode',
      connect_timeout=60,
      db='antipode',
      autocommit=True
    )
except pymysql.Error as e:
  print(f"[ERROR] MySQL exception: {e}")
  sys.exit()


def lambda_handler(event, context):
  # write post
  with mysql_conn.cursor() as cursor:
    # write with 0:AAAA -> blob of 1Mb
    # 1MB is the maximum packet size!!
    sql = "INSERT INTO `blobs` (`k`, `v`, `b`) VALUES (%s, %s, %s)"
    cursor.execute(sql, (int(event['i']), event['key'], os.urandom(1000000)))
    mysql_conn.commit()

  # returns OK with the k,v written
  event['written_at'] = str(datetime.now())

  # write notification to SNS topic
  client = boto3.client('sns')
  response = client.publish(
      TargetArn='arn:aws:sns:eu-central-1:641424397462:antipode',
      Message=json.dumps(event)
    )

  return {
    'statusCode': 200,
    'body': json.dumps(event, default=str)
  }