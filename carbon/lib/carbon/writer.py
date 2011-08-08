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


import time
from os.path import join, exists, dirname, basename

try:
  import cPickle as pickle
except ImportError:
  import pickle

from twisted.internet import reactor
from twisted.internet.task import LoopingCall
from twisted.application.service import Service
from carbon.cache import MetricCache
from carbon.storage import loadStorageSchemas
from carbon.conf import settings
from carbon.instrumentation import increment, append
from carbon import log
from ceres import CeresTree, setDefaultSliceCachingBehavior, NodeDeleted


Tree = CeresTree(settings.LOCAL_DATA_DIR)
setDefaultSliceCachingBehavior('latest')
nodeCache = {}


def writeCachedDataPoints():
  updates = 0
  lastSecond = 0

  for (metric, datapoints) in MetricCache.drain():
    node = nodeCache.get(metric)

    if node is None:
      node = nodeCache[metric] = Tree.getNode(metric)

      if node is None: # Create new node
        matchingSchema = None

        for schema in schemas:
          if schema.matches(metric):
            matchingSchema = schema
            break

        if matchingSchema is None:
          raise Exception("No storage schema matched the metric '%s', check your storage-schemas.conf file." % metric)

        node = nodeCache[metric] = Tree.createNode(metric, **matchingSchema.configuration)
        log.creates("created new metric %s with schema=%s" % (metric, matchingSchema.configurationString))
        increment('creates')

    try:
      t1 = time.time()
      node.write(datapoints)
      t2 = time.time()
    except NodeDeleted:
      del nodeCache[metric]
      continue
    except:
      log.err()
      increment('errors')
      continue
    
    updateTime = t2 - t1
    pointCount = len(datapoints)
    increment('committedPoints', pointCount)
    append('updateTimes', updateTime)

    if settings.LOG_UPDATES:
      log.updates("wrote %d datapoints for %s in %.5f seconds" % (pointCount, metric, updateTime))

    # Rate limit update operations
    thisSecond = int(t2)

    if thisSecond != lastSecond:
      lastSecond = thisSecond
      updates = 0
    else:
      updates += 1
      if updates >= settings.MAX_UPDATES_PER_SECOND:
        time.sleep( int(t2 + 1) - t2 )


def writeForever():
  while reactor.running:
    try:
      writeCachedDataPoints()
    except:
      log.err()

    time.sleep(1) # The writer thread only sleeps when the cache is empty or an error occurs


def reloadStorageSchemas():
  global schemas
  try:
    schemas = loadStorageSchemas()
  except:
    log.msg("Failed to reload storage schemas")
    log.err()


class WriterService(Service):

    def __init__(self):
        self.reload_task = LoopingCall(reloadStorageSchemas)

    def startService(self):
        self.reload_task.start(60, False)
        reactor.callInThread(writeForever)
        Service.startService(self)

    def stopService(self):
        self.reload_task.stop()
        Service.stopService(self)
