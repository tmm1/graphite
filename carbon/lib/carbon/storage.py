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

import os, re
from os.path import join, exists
from carbon.conf import OrderedConfigParser

try:
  import cPickle as pickle
except ImportError:
  import pickle


GRAPHITE_ROOT = os.environ['GRAPHITE_ROOT']
STORAGE_SCHEMAS_CONFIG = join(GRAPHITE_ROOT, 'conf', 'storage-schemas.conf')
STORAGE_LISTS_DIR = join(GRAPHITE_ROOT, 'storage', 'lists')


UnitMultipliers = {
  's' : 1,
  'm' : 60,
  'h' : 60 * 60,
  'd' : 60 * 60 * 24,
  'y' : 60 * 60 * 24 * 365,
}

def parseRetentionDefinition(retentionDef):
  (precision, points) = retentionDef.strip().split(':')

  if precision.isdigit():
    precisionUnit = 's'
    precision = int(precision)
  else:
    precisionUnit = precision[-1]
    precision = int( precision[:-1] )

  if points.isdigit():
    pointsUnit = None
    points = int(points)
  else:
    pointsUnit = points[-1]
    points = int( points[:-1] )

  if precisionUnit not in UnitMultipliers:
    raise ValueError("Invalid unit: '%s'" % precisionUnit)

  if pointsUnit not in UnitMultipliers and pointsUnit is not None:
    raise ValueError("Invalid unit: '%s'" % pointsUnit)

  precision = precision * UnitMultipliers[precisionUnit]

  if pointsUnit:
    points = points * UnitMultipliers[pointsUnit] / precision

  return (precision, points)



class Schema:
  def __init__(self, name, archives):
    self.name = name
    self.archives = sorted(archives)
    self.configuration = {
      'archives' : [ archive.config for archive in self.archives ],
      'timeStep' : self.archives[0].secondsPerPoint,
    }
    self.configurationString = ' '.join( '%s=%s' % item for item in sorted( self.configuration.items() ) )


  def test(self, metric):
    raise NotImplementedError()


  def matches(self, metric):
    return bool( self.test(metric) )



class DefaultSchema(Schema):
  def test(self, metric):
    return True



class PatternSchema(Schema):
  def __init__(self, name, archives, pattern):
    Schema.__init__(self, name, archives)
    self.pattern = pattern
    self.regex = re.compile(pattern)


  def test(self, metric):
    return self.regex.search(metric)



class ListSchema(Schema):
  def __init__(self, name, archives, listName):
    Schema.__init__(self, name, archives)
    self.listName = listName
    self.path = join(STORAGE_LISTS_DIR, listName)

    if exists(self.path):
      self.mtime = os.stat(self.path).st_mtime
      fh = open(self.path, 'rb')
      self.members = pickle.load(fh)
      fh.close()

    else:
      self.mtime = 0
      self.members = frozenset()


  def test(self, metric):
    if exists(self.path):
      current_mtime = os.stat(self.path).st_mtime

      if current_mtime > self.mtime:
        self.mtime = current_mtime
        fh = open(self.path, 'rb')
        self.members = pickle.load(fh)
        fh.close()

    return metric in self.members



class Archive:
  def __init__(self, secondsPerPoint, points):
    self.secondsPerPoint = int( secondsPerPoint )
    self.points = int( points )
    self.config = (self.secondsPerPoint, self.points)


  def getTuple(self):
    return (self.secondsPerPoint,self.points)


  @staticmethod
  def fromString(retentionDef):
    (secondsPerPoint, points) = parseRetentionDefinition(retentionDef)
    return Archive(secondsPerPoint, points)


  def __cmp__(self, other):
    return cmp(self.config, other.config)



def loadStorageSchemas():
  schemaList = []
  config = OrderedConfigParser()
  config.read(STORAGE_SCHEMAS_CONFIG)

  for section in config.sections():
    schemaName = section
    options = dict( config.items(section) )
    matchAll = options.get('match-all')
    pattern = options.get('pattern')
    listName = options.get('list')

    retentions = options['retentions'].split(',')
    archives = [ Archive.fromString(s) for s in retentions ]

    if matchAll:
      mySchema = DefaultSchema(schemaName, archives)

    elif pattern:
      mySchema = PatternSchema(schemaName, archives, pattern)

    elif listName:
      mySchema = ListSchema(schemaName, archives, listName)

    else:
      raise ValueError('schema "%s" has no pattern or list parameter configured' % schemaName)

    schemaList.append( mySchema )

  schemaList.append( defaultSchema )
  return schemaList


defaultArchive = Archive(60, 60 * 24 * 7) #default retention for unclassified data (7 days of minutely data)
defaultSchema = DefaultSchema('default', [defaultArchive])
