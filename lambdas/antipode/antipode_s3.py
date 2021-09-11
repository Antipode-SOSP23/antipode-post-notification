import os
import boto3
import botocore
import antipode as ant

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
    self.s3_client.put_object(
        Bucket=self.bucket,
        Key=self._bucket_key(c._id),
        Body=c.to_json(),
      )

  def retrieve_cscope(self, cscope_id, service_registry):
    # read cscope_id
    while True:
      try:
        s3_object = self.s3_client.get_object(Bucket=self.bucket, Key=self._bucket_key(str(cscope_id)))
        return ant.Cscope.from_json(service_registry, s3_object['Body'].read())
      except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] in ['NoSuchKey','404']:
          print(f"[RETRY] Read {self._bucket_key(str(cscope_id))}@{self.bucket}", flush=True)
          pass
        else:
          raise

  def cscope_barrier(self, operations):
    # read post operations
    for op in operations:
      # op: (BUCKET_NAME, <KEY>)
      while True:
        try:
          self.s3_client.head_object(Bucket=op[0], Key=op[1])
          break
        except botocore.exceptions.ClientError as e:
          if e.response['Error']['Code'] in ['NoSuchKey','404']:
            print(f"[RETRY] Read {op[1]}@{op[0]}", flush=True)
            pass
          else:
            raise
