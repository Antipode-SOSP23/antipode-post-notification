DELIMITED_BID_PREFIX = '_'
DELIMITER_COMPOSED_BID = ":"
DELIMITER_COMPOSED_ACSL = "/"
DELIMITER_ACSLS = ":"

def compute_bid(service_prefix, rid, index):
  return service_prefix + DELIMITED_BID_PREFIX + str(index) + DELIMITER_COMPOSED_BID + rid;

def next_acsls(acsl = "", num = 1):
  r = []
  for i in range(num):
    r.append(acsl + DELIMITER_ACSLS + str(i))
  return r

def compose_acsl(acsl_id = "", num = 1):
  return acsl_id + DELIMITER_COMPOSED_ACSL

# returns <acsl_id>, <num_sub_acsls>
def parse_acsl(acsl):
  return acsl.split(DELIMITER_COMPOSED_ACSL, 1)