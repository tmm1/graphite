"""Copyright 2009 Chris Davis

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License."""

from carbon.conf import settings
from carbon import log



class MetricCache(dict):
  def __init__(self):
    self.size = 0


  def __setitem__(self, key, value):
    raise TypeError("Use store() method instead!")


  def store(self, metric, datapoint):
    if self.isFull():
      return

    metric = '.'.join(part for part in metric.split('.') if part) # normalize the path

    try: # This is a hackish but very efficient technique for concurrent dict access without locking (the GIL does it for me)
      datapoints = dict.pop(self, metric)
    except KeyError:
      datapoints = []

    datapoints.append(datapoint)
    dict.__setitem__(self, metric, datapoints)
    self.size += 1


  def isFull(self):
    return self.size >= settings.MAX_CACHE_SIZE


  def pop(self, metric): # This raises a KeyError if a metric is concurrently having store() called on it, simply ignore and pop another
    datapoints = dict.pop(self, metric)
    self.size -= len(datapoints)
    return datapoints


  def drain(self):
    "Removes and generates metrics in order of most cached values to least"
    metrics = [ (metric, len(datapoints)) for metric,datapoints in self.items() ]
    metrics.sort(key=lambda item: item[1], reverse=True) # by queue size, descending

    for metric, queueSize in metrics:
      try: # metrics can momentarily disappear due to the implementation of our store() method
        datapoints = self.pop(metric)
      except KeyError:
        continue # we simply move on to the next metric when this race condition occurs

      yield (metric, datapoints)

  

MetricCache = MetricCache()
