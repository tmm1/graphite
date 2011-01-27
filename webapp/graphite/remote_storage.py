import socket
import time
import httplib
import traceback
from urllib import urlencode
from graphite.render.hashing import compactHash
from graphite.logger import log
from django.core.cache import cache
from django.conf import settings

try:
  import cPickle as pickle
except ImportError:
  import pickle



class RemoteStore(object):
  timeout = 5
  lastFailure = 0.0
  retryDelay = 10
  available = property(lambda self: time.time() - self.lastFailure > self.retryDelay)

  def __init__(self, host):
    self.host = host


  def find(self, query):
    request = FindRequest(self, query)
    request.send()
    return request


  def fail(self):
    self.lastFailure = time.time()



class FindRequest:
  suppressErrors = True

  def __init__(self, store, query):
    self.store = store
    self.query = query
    self.connection = None
    self.cacheKey = compactHash('find:%s:%s' % (self.store.host, query))
    self.cachedResults = None


  def send(self):
    log.info("FindRequest.send(host=%s, query=%s) called" % (self.store.host, self.query))
    self.cachedResults = cache.get(self.cacheKey)

    if self.cachedResults:
      log.info("FindRequest.send(host=%s, query=%s) returning cached results" % (self.store.host, self.query))
      return

    self.connection = HTTPConnectionWithTimeout(self.store.host)
    self.connection.timeout = self.store.timeout

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
      log.info("FindRequest.send(host=%s, query=%s) exception during request\n%s" % (self.store.host, self.query, traceback.format_exc()))
      self.store.fail()
      if not self.suppressErrors:
        raise


  def get_results(self):
    if self.cachedResults:
      return self.cachedResults

    if not self.connection:
      self.send()

    try:
      response = self.connection.getresponse()
      assert response.status == 200, "received error response %s - %s" % (response.status, response.reason)
      result_data = response.read()
      results = pickle.loads(result_data)

    except:
      log.info("FindRequest.get_results(host=%s, query=%s) exception processing response" % (self.store.host, self.query))
      self.store.fail()
      if not self.suppressErrors:
        raise
      else:
        results = []

    resultNodes = [ RemoteNode(self.store, node['metric_path'], node['isLeaf']) for node in results ]
    cache.set(self.cacheKey, resultNodes, settings.REMOTE_FIND_CACHE_DURATION)
    self.cachedResults = resultNodes
    return resultNodes



class RemoteNode:
  context = {}

  def __init__(self, store, metric_path, isLeaf):
    self.store = store
    self.fs_path = None
    self.metric_path = metric_path
    self.real_metric = metric_path
    self.name = metric_path.split('.')[-1]
    self.__isLeaf = isLeaf


  def fetch(self, startTime, endTime):
    if not self.isLeaf:
      raise Exception("Cannot fetch a non-leaf node")

    query_params = [
      ('target', self.metric_path),
      ('pickle', 'true'),
      ('from', str( int(startTime) )),
      ('until', str( int(endTime) ))
    ]
    query_string = urlencode(query_params)

    log.info("RemoteNode(%s).fetch(http://%s/render/?%s) sending request" % (self. metric_path, self.store.host, query_string))
    connection = HTTPConnectionWithTimeout(self.store.host)
    connection.timeout = self.store.timeout
    connection.request('GET', '/render/?' + query_string)
    response = connection.getresponse()

    if response.status != 200:
      log.info("RemoteNode(%s).fetch(http://%s/render/?%s) got error response %d %s" % (self. metric_path, self.store.host, query_string, response.status, response.reason))
      raise Exception("Failed to retrieve remote data: %d %s" % (response.status, response.reason))

    rawData = response.read()
    log.info("RemoteNode(%s).fetch(http://%s/render/?%s) got %d byte response" % (self. metric_path, self.store.host, query_string, len(rawData)))

    seriesList = pickle.loads(rawData)
    assert len(seriesList) == 1, "Invalid result: seriesList=%s" % str(seriesList)
    series = seriesList[0]

    timeInfo = (series['start'], series['end'], series['step'])
    return (timeInfo, series['values'])


  def isLeaf(self):
    return self.__isLeaf



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
