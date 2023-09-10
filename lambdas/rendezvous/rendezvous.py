import rendezvous_pb2 as rdv_proto

# ------------------------------
# context versioning propagation 
# ------------------------------

def context_msg_to_bytes(context):
  return context.SerializeToString()

def context_bytes_to_msg(context):
  message = rdv_proto.RequestContext()
  message.ParseFromString(context)
  return message

def context_msg_to_string(context):
  return context.SerializeToString().decode('utf-8')

def context_string_to_msg(context):
  message = rdv_proto.RequestContext()
  message.ParseFromString(context.encode('utf-8'))
  return message