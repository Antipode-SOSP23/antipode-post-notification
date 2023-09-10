import os
import sys

wait_registry = {
  'post-storage': getattr(sys.modules[f"antipode_{os.environ['POST_STORAGE']}"], 'wait')
}

def barrier(context):
  for storage,operations in context.c['operations'].items():
    wait_registry[storage](context._id, operations)

def fetch_operations(context, storage):
  if storage not in context.c['operations']:
    context.c['operations'][storage] = []
  return context.c['operations'][storage]

def append_operation(context, storage, wid):
  fetch_operations(context, storage).append(wid)