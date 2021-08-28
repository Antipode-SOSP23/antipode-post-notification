import json
import os
import boto3
import pymysql
import pymysql.cursors
from pprint import pprint
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
DYNAMO_NOTIFICATIONS_TABLE_NAME = os.environ.get('DYNAMO_NOTIFICATIONS_TABLE_NAME')
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

    with mysql_conn.cursor() as cursor:
      # write with 0:AAAA -> blob of 1Mb
      # 1MB is the maximum packet size!!
      sql = f"INSERT INTO `{MYSQL_POST_TABLE_NAME}` (`k`, `v`, `b`) VALUES (%s, %s, %s)"
      cursor.execute(sql, (int(event['i']), event['key'], os.urandom(1000000)))
      mysql_conn.commit()

    # returns OK with the k,v written
    event['written_at'] = str(datetime.utcnow())


    # write notification to current AWS region
    table_conn = boto3.resource('dynamodb',
        region_name=os.environ['AWS_REGION'],
        endpoint_url=f"http://dynamodb.{os.environ['AWS_REGION']}.amazonaws.com"
      ).Table(DYNAMO_NOTIFICATIONS_TABLE_NAME)
    table_conn.put_item(Item={'k': str(event['i']), 'v': str(event['key']), 'written_at': event['written_at']})

    # return the event and the code
    return {
      'statusCode': 200,
      'body': json.dumps(event, default=str)
    }
  except pymysql.Error as e:
    print(f"[ERROR] MySQL exception: {e}")
    exit(-1)
