import socket
import time
import httplib
from urllib import urlencode
from django.conf import settings
from django.core.cache import cache
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
      ('use_cache', '0'),
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
        reader = RemoteReader(self.store, node_info, bulk_query=self.query.pattern)
        yield LeafNode(node_info['path'], reader)
      else:
        yield BranchNode(node_info['path'])


class RemoteReader:
  request_cache = {}

  def __init__(self, store, node_info, bulk_query=None):
    self.store = store
    self.metric_path = node_info['path']
    self.intervals = node_info['intervals']
    self.query = bulk_query or node_info['path']

  def get_intervals(self):
    return self.intervals

  def fetch(self, startTime, endTime):
    query_params = [
      ('target', self.query),
      ('format', 'pickle'),
      ('local', '1'),
      ('noCache', '1'),
      ('from', str( int(startTime) )),
      ('until', str( int(endTime) ))
    ]
    query_string = urlencode(query_params)
    urlpath = '/render/?' + query_string

    results = self.request_data(self.store, urlpath)

    for series in results:
      if series['name'] == self.metric_path:
        time_info = (series['start'], series['end'], series['step'])
        return (time_info, series['values'])

  @classmethod
  def request_data(cls, store, urlpath):
    """This method allows multiple RemoteNodes (resulting from the same
    FindRequest) to share a single /render/ call when fetching their data.
    This is not thread-safe."""

    url = "http://%s%s" % (store.host, urlpath)
    cached_results = cls.request_cache.get(url)

    if cached_results is not None:
      return cached_results

    if len(cls.request_cache) >= settings.REMOTE_READER_CACHE_SIZE_LIMIT:
      log.info("RemoteReader.request_data :: clearing request_cache")
      cls.request_cache.clear()

    log.info("RemoteReader.request_data :: requesting %s" % url)
    connection = HTTPConnectionWithTimeout(store.host)
    connection.timeout = settings.REMOTE_FETCH_TIMEOUT

    try:
      connection.request('GET', urlpath)
      response = connection.getresponse()
      if response.status != 200:
        raise Exception("Error response %d %s from %s" % (response.status, response.reason, url))

      pickled_response = response.read()
      results = pickle.loads(pickled_response)
      cls.request_cache[url] = results
      return results

    except:
      log.exception("Error requesting %s" % url)
      store.fail()
      raise


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
