from twisted.protocols.basic import LineOnlyReceiver, Int32StringReceiver
from carbon.cache import MetricCache
from carbon.events import metricReceived
from carbon.util import LoggingMixin
from carbon import log

try:
  import cPickle as pickle
except ImportError:
  import pickle


class MetricLineReceiver(LoggingMixin, LineOnlyReceiver):
  delimiter = '\n'

  def lineReceived(self, line):
    try:
      metric, value, timestamp = line.strip().split()
      datapoint = ( float(timestamp), float(value) )
    except:
      log.listener('invalid line received from client %s, ignoring' % self.peerAddr)
      return

    increment('metricsReceived')
    metricReceived(metric, datapoint)



class MetricPickleReceiver(LoggingMixin, Int32StringReceiver):
  MAX_LENGTH = 2 ** 20

  def stringReceived(self, data):
    try:
      datapoints = pickle.loads(data)
    except:
      log.listener('invalid pickle received from client %s, ignoring' % self.peerAddr)
      return

    for (metric, datapoint) in datapoints:
      try:
        datapoint = ( float(datapoint[0]), float(datapoint[1]) ) #force proper types
      except:
        continue

      if datapoint[1] == datapoint[1]: # filter out NaN values
        metricReceived(metric, datapoint)

    increment('metricsReceived', len(datapoints))



class CacheQueryHandler(LoggingMixin, Int32StringReceiver):
  def stringReceived(self, metric):
    values = MetricCache.get(metric, [])
    log.query('cache query for %s returned %d values' % (metric, len(values)))
    response = pickle.dumps(values, protocol=-1)
    self.sendString(response)
    increment('cacheQueries')



# Avoid import circularity
from carbon.instrumentation import increment
