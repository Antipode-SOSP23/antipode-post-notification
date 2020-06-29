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
# Region: us-east-1 (N. Virginia)
# S3 Bucket: antipode-lambda-reader
# Stack name: antipode-lambda-reader
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
    FunctionName='arn:aws:lambda:eu-central-1:641424397462:function:antipode-lambda-writer-antipodelambdawriter-1HU9P3YKANACH',
    InvocationType='RequestResponse',
    Payload=json.dumps(event),
  )

  reply['evaluation']['ts_writer_reply'] = datetime.now()

  body = json.loads(response['Payload'].read()).get('body', {})
  if not body:
    print("[ERROR] Empty reply!")
  else:
    reply['writer_reply'] = json.loads(body)

    # read notification
    reply['evaluation']['ts_read_notf_start'] = datetime.now()
    print("[INFO] Reading notification", end='')
    table_conn = boto3.resource('dynamodb',
        region_name=os.environ['AWS_REGION'],
        endpoint_url=f"http://dynamodb.{os.environ['AWS_REGION']}.amazonaws.com"
      ).Table("keyvalue")

    while True:
      print('', end ='...')
      if 'Item' in table_conn.get_item(Key={'k': str(event['i'])}):
        break
      else:
        reply['evaluation']['read_notf_retries'] += 1

    reply['evaluation']['ts_read_notf_exit'] = datetime.now()
    print("[INFO] Done!")

    # read post
    print("[INFO] Reading post", end='')
    mysql_conn = pymysql.connect('antipode-dporto-cluster-1.cluster-ro-cdwqaw2esdz0.us-east-1.rds.amazonaws.com',
        port = 3306,
        user='admin',
        password='adminadmin',
        connect_timeout=5,
        db='antipode',
        autocommit=True
      )
    with mysql_conn.cursor() as cursor:
      while True:
        print('', end ='...')
        sql = "SELECT `b` FROM `keyvalue` WHERE `v`=%s"
        cursor.execute(sql, (event['key'],))
        result = cursor.fetchone()

        # current date and time
        if result is None:
          reply['evaluation']['read_post_retries'] += 1
        else:
          break

    reply['evaluation']['ts_read_post_exit'] = datetime.now()
    print("[INFO] Done!")

  return {
    'statusCode': 200,
    'body': json.dumps(reply, default=str)
  }
