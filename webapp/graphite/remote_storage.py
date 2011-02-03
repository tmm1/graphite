import socket
import time
import httplib
from urllib import urlencode
from django.conf import settings
from django.core import cache
from graphite.node import LeafNode, BranchNode
from graphite.logger import log

try:
  import cPickle as pickle
except ImportError:
  import pickle


class RemoteStore(object):
  lastFailure = 0.0
  available = property(lambda self: time.time() - self.lastFailure > settings.REMOTE_RETRY_DELAY)

  def __init__(self, host):
    self.host = host

  def find(self, query):
    request = FindRequest(self, query)
    request.send()
    return request

  def fail(self):
    self.lastFailure = time.time()


class FindRequest:
  def __init__(self, store, query):
    self.store = store
    self.query = query
    self.connection = None
    self.failed = False

    if query.startTime:
      start = query.startTime - (query.startTime % settings.FIND_CACHE_DURATION)
    else:
      start = ""

    if query.endTime:
      end = query.endTime - (query.endTime % settings.FIND_CACHE_DURATION)
    else:
      end = ""

    self.cacheKey = "find:%s:%s:%s:%s" % (store.host, query.pattern, start, end)
    self.cachedResult = None

  def send(self):
    log.info("FindRequest.send(host=%s, query=%s) called" % (self.store.host, self.query))

    self.cachedResult = cache.get(self.cacheKey)
    if self.cachedResult is not None:
      log.cache("FindRequest(host=%s, query=%s) using cached result" % (self.store.host, self.query))
      return

    self.connection = HTTPConnectionWithTimeout(self.store.host)
    self.connection.timeout = settings.REMOTE_FIND_TIMEOUT

    query_params = [
      ('local', '1'),
      ('format', 'pickle'),
      ('query', self.query.pattern),
    ]
    if self.query.startTime:
      query_params.append( ('from', self.query.startTime) )

    if self.query.endTime:
      query_params.append( ('until', self.query.endTime) )

    query_string = urlencode(query_params)

    try:
      self.connection.request('GET', '/metrics/find/?' + query_string)
    except:
      log.exception("FindRequest.send(host=%s, query=%s) exception during request" % (self.store.host, self.query))
      self.store.fail()
      self.failed = True

  def get_results(self):
    if self.failed:
      return

    if self.cachedResult is not None:
      results = self.cachedResult
    else:
      if self.connection is None:
        self.send()

      try:
        response = self.connection.getresponse()
        assert response.status == 200, "received error response %s - %s" % (response.status, response.reason)
        result_data = response.read()
        results = pickle.loads(result_data)

      except:
        log.exception("FindRequest.get_results(host=%s, query=%s) exception processing response" % (self.store.host, self.query))
        self.store.fail()
        return

      cache.set(self.cacheKey, results, settings.FIND_CACHE_DURATION)

    for node_info in results:
      if node_info['is_leaf']:
        reader = RemoteReader(self.store, node_info)
        yield LeafNode(node_info['path'], reader)
      else:
        yield BranchNode(node_info['path'])


class RemoteReader:
  def __init__(self, store, node_info):
    self.store = store
    self.metric_path = node_info['path']
    self.intervals = node_info['intervals']

  def get_intervals(self):
    return self.intervals

  def fetch(self, startTime, endTime):
    query_params = [
      ('target', self.metric_path),
      ('pickle', 'true'),
      ('from', str( int(startTime) )),
      ('until', str( int(endTime) ))
    ]
    query_string = urlencode(query_params)

    url = "http://%s/render/?%s" % (self.store.host, query_string)
    log.info("RemoteReader.fetch %s" % url)

    connection = HTTPConnectionWithTimeout(self.store.host)
    connection.timeout = settings.REMOTE_FETCH_TIMEOUT
    connection.request('GET', '/render/?' + query_string)
    response = connection.getresponse()

    if response.status != 200:
      raise Exception("Error response %d %s from %s" % (response.status, response.reason, url))

    pickled_response = response.read()
    series_list = pickle.loads(pickled_response)
    series = series_list[0]
    time_info = (series['start'], series['end'], series['step'])
    return (time_info, series['values'])


# This is a hack to put a timeout in the connect() of an HTTP request.
# Python 2.6 supports this already, but many Graphite installations
# are not on 2.6 yet.

class HTTPConnectionWithTimeout(httplib.HTTPConnection):
  timeout = 30

  def connect(self):
    msg = "getaddrinfo returns an empty list"
    for res in socket.getaddrinfo(self.host, self.port, 0, socket.SOCK_STREAM):
      af, socktype, proto, canonname, sa = res
      try:
        self.sock = socket.socket(af, socktype, proto)
        try:
          self.sock.settimeout( float(self.timeout) ) # default self.timeout is an object() in 2.6
        except:
          pass
        self.sock.connect(sa)
        self.sock.settimeout(None)
      except socket.error, msg:
        if self.sock:
          self.sock.close()
          self.sock = None
          continue
      break
    if not self.sock:
      raise socket.error, msg
