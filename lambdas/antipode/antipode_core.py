import os
import importlib

class AntipodeCore:
  wait_registry = {
    'post-storage': getattr(importlib.import_module(f"antipode_{os.environ['POST_STORAGE']}"), 'wait')
  }

  def barrier(context):
    for storage,operations in context.c['operations'].items():
      AntipodeCore.wait_registry[storage](operations)

  def append_operation(context, storage, wid):
    AntipodeCore.fetch_operations(context, storage).append(wid)

  def fetch_operations(context, storage):
    if storage not in context.c['operations']:
      context.c['operations'][storage] = []
    return context.c['operations'][storage]