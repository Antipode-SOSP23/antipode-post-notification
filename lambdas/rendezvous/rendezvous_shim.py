from abc import abstractmethod
import os
import grpc
import threading
import rendezvous_pb2 as rdv_proto
import rendezvous_pb2_grpc as rdv_service
import time
from datetime import datetime, timedelta

class RendezvousShim:
  def __init__(self, region):
    self.region = region

    self.rendezvous_address = os.environ[f"RENDEZVOUS_{region.replace('-','_').upper()}"]
    self.channel = grpc.insecure_channel(self.rendezvous_address)
    self.stub = rdv_service.RendezvousServiceStub(self.channel)

    self.metadata = {}
    self.metadata_lock = threading.Lock()

    # Evaluation purposes
    self.inconsistency = False

# ----------------
# Current request
# ----------------

  @abstractmethod
  def read_metadata(self, item):
    pass

  def close_branch(self, rid):
      bid = self.read_metadata(rid)
      try:
        self.stub.closeBranch(rdv_proto.CloseBranchMessage(rid=rid, bid=bid, region=self.region))
        print("[INFO] Closed branch with prevented inconsistency =", self.inconsistency)
      except grpc.RpcError as e:
        print(f"[ERROR] Rendezvous exception closing branch: {e.details()}")
        exit(-1)


# -------------
# All requests
# -------------

  @abstractmethod
  def _parse_metadata(self, item):
    pass

  @abstractmethod
  def read_all_metadata(self, item):
    pass

  def clean_expired_metadata(self):
    while True:
      with self.metadata_lock:
        now = datetime.now()

        for rid in list(self.metadata.keys()):
          ts = self.metadata[rid]

          # delete if created more than 30 minutes ago
          if now - ts > timedelta(minutes=30):
            del self.metadata[rid]

      # sleep for 30 minutes
      time.sleep(1800) 

  def close_branches(self):
    now = datetime.now()

    while True:
      with self.metadata_lock:
        items = self.read_all_metadata()

        for item in items:
          rid, bid = self._parse_metadata(item)

          if rid not in self.metadata:
            try:
              self.stub.closeBranch(rdv_proto.CloseBranchMessage(rid=rid, bid=bid, region=self.region))
              self.metadata[rid] = now

            except grpc.RpcError as e:
              print(f"[ERROR] Rendezvous exception closing branch: {e.details()}", flush=True)
              exit(-1)
