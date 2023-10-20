def compute_bid(service_prefix, rid, index):
  return service_prefix + '_' + str(index) + ':' + rid;

def next_async_zones(async_zone = "", num = 1):
  r = []
  for i in range(num):
    r.append(async_zone + ':' + str(i))
  return r
  