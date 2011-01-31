import time
from ceres import CeresNode
from graphite.node import LeafNode, BranchNode


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



class CeresReader:
  supported = True

  def __init__(self, fs_path):
    self.node = CeresNode.fromFilesystemPath(fs_path)

  def fetch(self, fromTime, untilTime):
    data = self.node.read(fromTime, untilTime)
    timeInfo = (data.startTime, data.endTime, data.timeStep)
    return (timeInfo, data.values)


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
