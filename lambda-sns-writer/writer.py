import json
import os
import boto3
import pymysql
import pymysql.cursors
from pprint import pprint

#---------------
# AWS SAM Deployment details
#
# Region: eu-central-1 (Frankfurt)
# S3 Bucket: antipode-lambda-sns-writer
# Stack name: antipode-lambda-sns-writer
#---------------

def lambda_handler(event, context):
  # write post
  mysql_conn = pymysql.connect('antipode-dporto-cluster-1.cluster-citztxl8ztvl.eu-central-1.rds.amazonaws.com',
      port = 3306,
      user='admin',
      password='adminadmin',
      connect_timeout=5,
      db='antipode',
      autocommit=True
    )
  with mysql_conn.cursor() as cursor:
    # write with 0:AAAA -> blob of 1Mb
    sql = "INSERT INTO `keyvalue` (`k`, `v`, `b`) VALUES (%s, %s, %s)"
    cursor.execute(sql, (int(event['i']), event['key'], os.urandom(10000000)))
    mysql_conn.commit()


  # write notification
  client = boto3.client('sns')
  response = client.publish(
      TargetArn='arn:aws:sns:eu-central-1:641424397462:antipode',
      Message=json.dumps(event)
    )

  # returns OK with the k,v written
  reply = {
    'role': 'writer',
    'key': event['i'],
    'value': event['key'],
  }
  return {
    'statusCode': 200,
    'body': json.dumps(reply, default=str)
  }