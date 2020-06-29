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
# Cloud Formation Stack name: antipode-lambda-sns-writer
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
    cursor.execute(sql, (event['i'], event['key'], os.urandom(1000000)))
    mysql_conn.commit()


  # write notification
  table_conn = boto3.resource('dynamodb',
      region_name=os.environ['AWS_REGION'],
      endpoint_url=f"http://dynamodb.{os.environ['AWS_REGION']}.amazonaws.com"
    ).Table("keyvalue")
  table_conn.put_item(Item={'k': str(event['i']), 'v': str(event['key'])})


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
