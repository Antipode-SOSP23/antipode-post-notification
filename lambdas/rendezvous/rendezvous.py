DELIMITED_BID_PREFIX = '_'
DELIMITER_COMPOSED_BID = ":"
DELIMITER_COMPOSED_ZONE = "/"
DELIMITER_SUB_ZONES = ":"

def compute_bid(service_prefix, rid, index):
  return service_prefix + DELIMITED_BID_PREFIX + str(index) + DELIMITER_COMPOSED_BID + rid;

def next_async_zones(async_zone = "", num = 1):
  r = []
  for i in range(num):
    r.append(async_zone + DELIMITER_SUB_ZONES + str(i))
  return r

def compose_async_zone(async_zone_id = "", num = 1):
  return async_zone_id + DELIMITER_COMPOSED_ZONE

# returns <async_zone_id>, <num_sub_async_zones>
def parse_async_zone(async_zone):
  return async_zone.split(DELIMITER_COMPOSED_ZONE, 1)