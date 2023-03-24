from abc import abstractmethod
import os
import grpc
import threading
import rendezvous_pb2 as rdv_pb
import rendezvous_pb2_grpc as rdv_service
import time
from datetime import datetime, timedelta

RENDEZVOUS_ADDRESS = os.environ['RENDEZVOUS_ADDRESS']

class RendezvousShim:
  def __init__(self, region):
    self.region = region

    # Rendezvous
    self.channel = grpc.insecure_channel(RENDEZVOUS_ADDRESS)
    self.stub = rdv_service.RendezvousServiceStub(self.channel)

    # Tracking requests read so far -> key: rid, value: timestamp
    self.metadata = {}
    self.metadata_lock = threading.Lock()

  def clean_expired_metadata(self):
    while True:
      with self.metadata_lock:
        now = datetime.now()

        for rid in list(self.metadata.keys()):
          ts = self.metadata[rid]

          # delete if created more than 60 minutes ago
          if now - ts > timedelta(minutes=60):
            del self.metadata[rid]

      # sleep for 60 minutes
      time.sleep(3600) 

  def close_branches(self):
    while True:
      with self.metadata_lock:
        items = self._read_metadata()

        for item in items:
          rid, service, ts = self._parse_metadata(item)

          # only close branch once
          if rid not in self.metadata:
            try:
              self.stub.closeBranch(rdv_pb.CloseBranchMessage(rid=rid, service=service, region=self.region))

            except grpc.RpcError as e:
              print(f"[ERROR] Rendezvous exception closing branch: {e.details()}", flush=True)
              # for the sake of this benchmark:
              if e.code() == grpc.StatusCode.INVALID_ARGUMENT:
                # server was restarted so we ignore, otherwise we will encounter multiple exceptions
                pass
              elif e.code() == grpc.StatusCode.UNAVAILABLE:
                # force all threads to exit after stopping server
                exit(-1)
              else:
                continue

            # keep track of the keys we have read
            self.metadata[rid] = self._parse_time(ts)

  def _parse_time(self, time_str):
    # return value in time format
    return datetime.strptime(time_str, '%Y-%m-%d %H:%M')

  @abstractmethod
  def _parse_metadata(self, item):
    pass
  
  @abstractmethod
  def _read_metadata(self, item):
    pass
