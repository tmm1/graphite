try:
  from hashlib import md5
except ImportError:
  from md5 import md5
import bisect
from carbon.conf import settings


serverRing = None
ports = {}


class ConsistentHashRing:
  def __init__(self, nodes, replica_count=100):
    self.ring = []
    self.nodes = set()
    self.replica_count = replica_count
    for node in nodes:
      self.add_node(node)

  def compute_ring_position(self, key):
    big_hash = md5( str(key) ).hexdigest()
    small_hash = int(big_hash[:4], 16)
    return small_hash

  def add_node(self, key):
    self.nodes.add(key)
    for i in range(self.replica_count):
      replica_key = "%s:%d" % (key, i)
      position = self.compute_ring_position(replica_key)
      entry = (position, key)
      bisect.insort(self.ring, entry)

  def remove_node(self, key):
    self.nodes.discard(key)
    self.ring = [entry for entry in self.ring if entry[1] != key]

  def get_node(self, key):
    assert self.ring
    position = self.compute_ring_position(key)
    search_entry = (position, None)
    index = bisect.bisect_left(self.ring, search_entry) % len(self.ring)
    entry = self.ring[index]
    return entry[1]

  def get_nodes(self, key):
    nodes = []
    position = self.compute_ring_position(key)
    search_entry = (position, None)
    index = bisect.bisect_left(self.ring, search_entry) % len(self.ring)
    last_index = (index - 1) % len(self.ring)
    while len(nodes) < len(self.nodes) and index != last_index:
      next_entry = self.ring[index]
      (position, next_node) = next_entry
      if next_node not in nodes:
        nodes.append(next_node)

      index = (index + 1) % len(self.ring)

    return nodes


def setDestinationHosts(hosts):
  global serverRing
  servers = set()
  for (server, port, instance) in hosts:
    servers.add(server)
    ports[ (server, instance) ] = port

  if len(servers) < settings.REPLICATION_FACTOR:
    raise Exception("REPLICATION_FACTOR=%d cannot exceed servers=%d" % (settings.REPLICATION_FACTOR, len(servers)))

  serverRing = ConsistentHashRing(ports)


def getDestinations(metric):
  count = 0
  used = set()
  for host in serverRing.get_nodes(metric):
    (server, instance) = host
    if server in used:
      continue
    port = ports[host]
    yield (server, port)
    used.add(server)
    count += 1
    if count >= settings.REPLICATION_FACTOR:
      return
