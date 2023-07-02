import threading
import rendezvous_pb2 as rdv_proto

class Rendezvous:
  def __init__(self, shim_layer):
    self.shim_layer = shim_layer
    self.threads = []

  def close_subscribed_branches(self):
    t = threading.Thread(target=self.shim_layer.close_subscribed_branches)
    self.threads.append(t)
    t.start()

  def close_branch(self, bid):
    t = threading.Thread(target=self.shim_layer.close_branch, args=(bid,))
    self.threads.append(t)
    t.start()

  # Specific for this benchmark
  def prevented_inconsistency(self):
    return self.shim_layer.inconsistency
  
  def stop(self):
    self.shim.running = False
    for t in self.threads:
      t.join()


def context_proto_to_bytes(context):
  return context.SerializeToString()

def context_bytes_to_proto(context):
  message = rdv_proto.RequestContext()
  message.ParseFromString(context)
  return message

def context_proto_to_string(context):
  return context.SerializeToString().decode('utf-8')

def context_string_to_proto(context):
  message = rdv_proto.RequestContext()
  message.ParseFromString(context.encode('utf-8'))
  return message