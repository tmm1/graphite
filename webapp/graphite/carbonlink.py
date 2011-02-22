import socket
import struct
import errno
from select import select
from django.conf import settings
from graphite.logger import log

try:
  import cPickle as pickle
except ImportError:
  import pickle


class CarbonLink:
  def __init__(self):
    self.host = settings.CARBONLINK_HOST
    self.port = settings.CARBONLINK_PORT
    self.timeout = settings.CARBONLINK_TIMEOUT
    self.connection_pool = set()

  def get_connection(self):
    while self.connection_pool:
      try:
        conn = self.connection_pool.pop()
      except KeyError: # for thread-safety
        break

      if still_connected(conn):
        return conn

    log.cache("No available connections in CarbonLink pool, creating new connection")
    sock = socket.socket()
    sock.settimeout(self.timeout)
    sock.connect( (self.host, self.port) )
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
    return sock

  def query(self, metric_path):
    conn = self.get_connection()
    self.send_request(conn, metric_path)
    results = self.recv_response(conn)
    log.cache("CarbonLink query for %s returned %d datapoints" % (metric_path, len(results)))
    self.connection_pool.add(conn)
    return results

  def send_request(self, conn, metric_path):
    len_prefix = struct.pack("!L", len(metric_path))
    request_packet = len_prefix + metric_path
    conn.sendall(request_packet)

  def recv_response(self, conn):
    len_prefix = recv_exactly(conn, 4)
    body_size = struct.unpack("!L", len_prefix)[0]
    body = recv_exactly(conn, body_size)
    return pickle.loads(body)

    connection = socket.socket()
    connection.settimeout(self.timeout)
    connection.connect(host)
    return connection



# Socket helper functions
def still_connected(sock):
  is_readable = select([sock], [], [], 0)[0]
  if is_readable:
    try:
      recv_buf = sock.recv(1, socket.MSG_DONTWAIT|socket.MSG_PEEK)

    except socket.error, e:
      if e.errno in (errno.EAGAIN, errno.EWOULDBLOCK):
        return True
      else:
        raise

    else:
      return bool(recv_buf)

  else:
    return True


def recv_exactly(conn, num_bytes):
  buf = ''
  while len(buf) < num_bytes:
    data = conn.recv( num_bytes - len(buf) )
    if not data:
      raise Exception("Connection lost")
    buf += data

  return buf


#Ghetto singleton
CarbonLink = CarbonLink()
