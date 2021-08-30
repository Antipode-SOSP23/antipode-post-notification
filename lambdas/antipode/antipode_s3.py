import os
import boto3
import botocore

S3_ANTIPODE_PATH = os.environ['S3_ANTIPODE_PATH']

class AntipodeS3:
  def __init__(self, _id, conn):
    self._id = _id
    self.bucket = conn
    self.s3_client = boto3.client('s3')

  def _id(self):
    return self._id

  def _bucket_key(self, cid):
    # if cid is None use this cscope id
    return f"{S3_ANTIPODE_PATH}/{str(cid)}"


  def cscope_close(self, c):
    # write post
    self.s3_client.put_object(
        Bucket=self.bucket,
        Key=self._bucket_key(c._id),
        Body='', # we could add more info as json
      )

  def cscope_barrier(self, cscope_id, operations):
    # read cscope_id
    while True:
      try:
        self.s3_client.get_object(Bucket=self.bucket, Key=self._bucket_key(str(cscope_id)))
        break
      except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
          print(f"[RETRY] Read {self._bucket_key(str(cscope_id))}@{self.bucket}")
          pass
        else:
          raise


    # read post operations
    for op in operations:
      # op: (BUCKET_NAME, <KEY>)
      while True:
        try:
          self.s3_client.get_object(Bucket=op[0], Key=op[1])
          break
        except botocore.exceptions.ClientError as e:
          if e.response['Error']['Code'] == 'NoSuchKey':
            print(f"[RETRY] Read {op[1]}@{op[0]}")
            pass
          else:
            raise
