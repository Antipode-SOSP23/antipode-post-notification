import json
import os
import boto3
import pymysql
import pymysql.cursors
from pprint import pprint
import time

#---------------
# AWS SAM Deployment details
#
# Region: eu-central-1 (Frankfurt)
# S3 Bucket: antipode-lambda-sns-writer
# Stack name: antipode-lambda-sns-writer
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
    sql = "INSERT INTO `keyvalue` (`k`, `v`, `b`) VALUES (%s, %s, %s)"
    cursor.execute(sql, (int(event['i']), event['key'], os.urandom(1000000)))

  mysql_conn.commit()

  # write notification to SNS topic
  client = boto3.client('sns')
  response = client.publish(
      TargetArn='arn:aws:sns:eu-central-1:641424397462:antipode',
      Message=json.dumps(event)
    )

  # returns OK with the k,v written
  return {
    'statusCode': 200,
    'body': json.dumps(event, default=str)
  }