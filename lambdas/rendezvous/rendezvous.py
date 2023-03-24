import threading

class Rendezvous:
  def __init__(self, service_registry):
    self.service_registry = service_registry

  def init_polling(self):
    t1 = threading.Thread(target=self.service_registry.close_branches)
    t2 = threading.Thread(target=self.service_registry.clean_expired_metadata)
    t1.start()
    t2.start()