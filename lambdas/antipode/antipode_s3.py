import os
import boto3

S3_ANTIPODE_PATH = os.environ['S3_ANTIPODE_PATH']

class AntipodeS3:
  def __init__(self, _id, bucket):
    self._id = _id
    self.bucket = bucket
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
      self.s3_client.get_object(Bucket=self.bucket, Key=self._bucket_key(str(cscope_id)))
      break

    # read post operations
    for op in operations:
      # op: (BUCKET_NAME, <KEY>)
      while True:
        self.s3_client.get_object(Bucket=op[0], Key=op[1])
        break
