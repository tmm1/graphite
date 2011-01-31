import time
from graphite.node import LeafNode, BranchNode
from graphite.carbonlink import CarbonLink
from graphite.logger import log


try:
  import whisper
except ImportError:
  whisper = False

try:
  import rrdtool
except ImportError:
  rrdtool = False

try:
  import gzip
except ImportError:
  gzip = False



class MultiReader:
  def __init__(self, nodes):
    self.nodes = nodes

  def fetch(self, startTime, endTime):
    results = [ n.fetch(startTime, endTime) for n in self.nodes ]
    return reduce(results, self.merge)

  def merge(self, results1, results2):
    pass #XXX call node.fetch() on each node, and surgically combine the results.
    #first dilemma is to figure out picking a step... I could default to the finest
    #  but i'd have to keep coarser ones correct by repeating datapoints (which could get hairy...)


class CeresReader:
  supported = True

  def __init__(self, ceres_node, real_metric_path):
    self.ceres_node = ceres_node
    self.real_metric_path = real_metric_path

  def fetch(self, startTime, endTime):
    data = self.ceres_node.read(startTime, endTime)
    time_info = (data.startTime, data.endTime, data.timeStep)
    values = list(data.values)

    # Merge in data from carbon's cache
    if data.endTime < endTime:
      try:
        cached_datapoints = CarbonLink.query(self.real_metric_path)
      except:
        log.exception("Failed CarbonLink query '%s'" % self.real_metric_path)
        cached_datapoints = []

      for (timestamp, value) in cached_datapoints:
        interval = timestamp - (timestamp % data.timeStep)

        try:
          i = int(interval - data.startTime) / data.timeStep
          values[i] = value
        except:
          pass

    return (time_info, values)


class WhisperReader:
  supported = bool(whisper)

  def __init__(self, fs_path):
    self.fs_path = fs_path

  def fetch(self, startTime, endTime):
    return whisper.fetch(self.fs_path, startTime, endTime)


class GzippedWhisperReader(WhisperReader):
  supported = bool(whisper and gzip)

  def fetch(self, startTime, endTime):
    fh = gzip.GzipFile(self.fs_path, 'rb')
    try:
      return whisper.file_fetch(fh, startTime, endTime)
    finally:
      fh.close()


class RRDFileReader:
  supported = bool(rrdtool)

  def __init__(self, fs_path):
    self.fs_path = fs_path

  def getDataSources(self):
    try:
      info = rrdtool.info(self.fs_path)
      return [RRDDataSourceReader(self, source) for source in info['ds']]
    except:
      raise
      return []


class RRDDataSourceReader:
  supported = RRDFileReader.supported

  def __init__(self, rrd_file, name):
    self.rrd_file = rrd_file
    self.name = name

  def fetch(self, startTime, endTime):
    startString = time.strftime("%H:%M_%Y%m%d", time.localtime(startTime))
    endString = time.strftime("%H:%M_%Y%m%d", time.localtime(endTime))

    (timeInfo, columns, rows) = rrdtool.fetch(self.rrd_file.fs_path,'AVERAGE','-s' + startString,'-e' + endString)
    colIndex = list(columns).index(self.name)
    rows.pop() #chop off the latest value because RRD returns crazy last values sometimes
    values = (row[colIndex] for row in rows)

    return (timeInfo, values)
