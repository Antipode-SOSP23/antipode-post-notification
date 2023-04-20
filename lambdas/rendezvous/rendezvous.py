import threading
import rendezvous_pb2 as rdv_proto

class Rendezvous:
  def __init__(self, shim_layer):
    self.shim_layer = shim_layer

  def init_polling(self):
    t1 = threading.Thread(target=self.shim_layer.close_branches)
    t2 = threading.Thread(target=self.shim_layer.clean_expired_metadata)
    t1.start()
    t2.start()

  def init_close_branch(self, rid):
    t = threading.Thread(target=self.shim_layer.close_branch, args=(rid,))
    t.start()

  def prevented_inconsistency(self):
    return self.shim_layer.inconsistency


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