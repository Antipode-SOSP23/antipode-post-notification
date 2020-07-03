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
# S3 Bucket: antipode-lambda-eu
# Stack name: antipode-lambda-sql-only-writer
#
# Payload Example:
# { "i": "1", "key": "AABB11" }
#---------------




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
# S3 Bucket: antipode-lambda-eu
# Stack name: antipode-lambda-sql-only-writer
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
  exit(-1)


def lambda_handler(event, context):
  with mysql_conn.cursor() as cursor:
    # write post
    # write with 0:AAAA -> blob of 1Mb
    # 1MB is the maximum packet size!!
    sql = "INSERT INTO `blobs` (`k`, `v`, `b`) VALUES (%s, %s, %s)"
    cursor.execute(sql, (int(event['i']), event['key'], os.urandom(1000000)))
    mysql_conn.commit()

    # write notif
    # write with 0:AAAA -> blob of 1Mb
    # 1MB is the maximum packet size!!
    sql = "INSERT INTO `keyvalue` (`k`, `v`) VALUES (%s, %s)"
    cursor.execute(sql, (int(event['i']), event['key']))
    mysql_conn.commit()

  # returns OK with the k,v written
  return {
    'statusCode': 200,
    'body': {
      'key': event['i'],
      'value': event['key'],
    }
  }
