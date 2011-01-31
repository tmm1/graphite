import os
import time
import fnmatch
import socket
import errno
from os.path import islink, isdir, isfile, realpath, join, dirname, basename
from django.conf import settings
from ceres import CeresNode
from graphite.node import BranchNode, LeafNode
from graphite.intervals import Interval
from graphite.remote_storage import RemoteStore
from graphite.readers import CeresReader, WhisperReader, GzippedWhisperReader, RRDFileReader
from graphite.logger import log


DATASOURCE_DELIMETER = '::RRD_DATASOURCE::'


class Store:
  def __init__(self, directories=[], hosts=[]):
    self.directories = directories
    self.remote_hosts = [host for host in hosts if not is_local_interface(host) ]
    self.remote_stores = [ RemoteStore(host) for host in self.remote_hosts ]

    if not (directories or remote_hosts):
      raise ValueError("directories and remote_hosts cannot both be empty")


  def find(self, pattern, startTime=None, endTime=None):
    query = Query(pattern, startTime, endTime)

    # Start remote searches
    remote_requests = [ r.find(query) for r in self.remote_stores if r.available ]
    matching_nodes = set()

    # Search locally
    for directory in self.directories:
      for node in find_nodes(directory, query):
        matching_nodes.add(node)

    # Gather remote search results
    for request in remote_requests:
      for node in request.get_results():
        matching_nodes.add(node)

    # Group matching nodes by their metric path
    nodes_by_metric = {}
    for node in matching_nodes:
      if node.metric_path not in nodes_by_metric:
        nodes_by_metric[node.metric_path] = set()

      nodes_by_metric[node.metric_path].add(node)

    # Reduce matching nodes for each metric to a minimal set
    for metric, nodes in nodes_by_metric.iteritems():
      minimal_node_set = set()
      covered_intervals = IntervalSet([])

      def measure_of_added_coverage(node):
        relevant_intervals = node.intervals.intersect_interval(query.interval)
        relevant_intervals -= covered_intervals
        return relevant_intervals.size

      while nodes:
        best_node = max(nodes, key=measure_of_added_coverage)

        if measure_of_added_coverage(best_node) == 0:
          break

        nodes.remove(best_node)
        minimal_node_set.add(node)
        covered_intervals = covered_intervals.union(node.intervals)

      # Sometimes the requested interval falls within the caching window.
      # We include the most likely node if the gap is within tolerance.
      if not minimal_node_set:
        def distance_to_requested_interval(node):
          latest = sorted(nodes.intervals, key=lambda i: i.end)[-1]
          distance = query.interval.start - latest.end
          return distance if distance >= 0 else float('inf')

        best_candidate = min(nodes, key=distance_to_requested_interval)
        if distance_to_requested_interval(best_candidate) <= settings.FIND_TOLERANCE:
          minimal_node_set.add(best_candidate)

      yield MetaNode(metric, minimal_nodes)



class Query:
  isExact = property(lambda self: '*' not in self.pattern and
                                  '?' not in self.pattern and
                                  '[' not in self.pattern)

  def __init__(self, pattern, startTime, endTime):
    self.pattern = pattern
    self.startTime = startTime
    self.endTime = endTime
    self.interval = Interval(float('-inf') if startTime is None else startTime,
                             float('inf') if endTime is None else endTime)


  def __repr__(self):
    if self.startTime is None:
      startString = '*'
    else:
      startString = time.ctime(self.startTime)

    if self.endTime is None:
      endString = '*'
    else:
      endString = time.ctime(self.endTime)

    return '<Query: %s from %s until %s>' % (self.pattern, startString, endString)



def is_local_interface(host):
  if ':' in host:
    host = host.split(':',1)[0]

  for port in xrange(1025, 65535):
    try:
      sock = socket.socket()
      sock.bind( (host,port) )
      sock.close()

    except socket.error, e:
      if e.args[0] == errno.EADDRNOTAVAIL:
        return False
      else:
        continue

    else:
      return True

  raise Exception("Failed all attempts at binding to interface %s, last exception was %s" % (host, e))



def fs_to_metric(path):
  dirpath = dirname(path)
  filename = basename(path)
  return join(dirpath, filename.split('.')[0]).replace('/','.')



def find_nodes(root_dir, query):
  "Generates nodes beneath root_dir matching the given pattern"
  pattern_parts = query.pattern.split('.')

  for absolute_path in _find_paths(root_dir, pattern_parts):
    if basename(absolute_path).startswith('.'):
      continue

    if DATASOURCE_DELIMETER in basename(absolute_path):
      (absolute_path, datasource_pattern) = absolute_path.rsplit(DATASOURCE_DELIMETER, 1)
    else:
      datasource_pattern = None

    relative_path = absolute_path[ len(root_dir): ].lstrip('/')
    metric_path = fs_to_metric(relative_path)

    # Support symbolic links (real_metric_path ensures proper cache queries)
    if islink(absolute_path):
      real_fs_path = realpath(absolute_path)
      relative_fs_path = metric_path.replace('.', '/')
      base_fs_path = absolute_path[ :-len(relative_fs_path) ]
      relative_real_fs_path = real_fs_path[ len(base_fs_path): ]
      real_metric_path = fs_to_metric( relative_real_fs_path )
    else:
      real_metric_path = metric_path

    # Now we construct and yield an appropriate Node object
    if isdir(absolute_path):
      if CeresNode.isNodeDir(absolute_path):
        reader = CeresReader(absolute_path)
        if reader.node.hasDataForInterval(query.startTime, query.endTime):
          yield LeafNode(metric_path, real_metric_path, reader)

      else:
        yield BranchNode(metric_path)

    elif isfile(absolute_path):
      if absolute_path.endswith('.wsp') and WhisperReader.supported:
        reader = WhisperReader(absolute_path)
        yield LeafNode(metric_path, real_metric_path, reader)

      elif absolute_path.endswith('.wsp.gz') and GzippedWhisperReader.supported:
        reader = GzippedWhisperReader(absolute_path)
        yield LeafNode(metric_path, real_metric_path, reader)

      elif absolute_path.endswith('.rrd') and RRDFileReader.supported:
        reader = RRDFileReader(absolute_path)

        if datasource_pattern is None:
          yield BranchNode(metric_path)

        else:
          for source in reader.getDataSources():
            if fnmatch.fnmatch(source.name, datasource_pattern):
              yield source


def _find_paths(current_dir, patterns):
  """Recursively generates absolute paths whose components underneath current_dir
  match the corresponding pattern in patterns"""
  pattern = patterns[0]
  patterns = patterns[1:]
  entries = os.listdir(current_dir)

  subdirs = [e for e in entries if isdir( join(current_dir,e) )]
  matching_subdirs = fnmatch.filter(subdirs, pattern)
  matching_subdirs.sort()

  if len(patterns) == 1 and rrdtool: #the last pattern may apply to RRD data sources
    files = [e for e in entries if isfile( join(current_dir,e) )]
    rrd_files = fnmatch.filter(files, pattern + ".rrd")
    rrd_files.sort()

    if rrd_files: #let's assume it does
      datasource_pattern = patterns[0]

      for rrd_file in rrd_files:
        absolute_path = join(current_dir, rrd_file)
        yield absolute_path + DATASOURCE_DELIMETER + datasource_pattern

  if patterns: #we've still got more directories to traverse
    for subdir in matching_subdirs:

      absolute_path = join(current_dir, subdir)
      for match in _find_paths(absolute_path, patterns):
        yield match

  else: #we've got the last pattern
    files = [e for e in entries if isfile( join(current_dir,e) )]
    matching_files = fnmatch.filter(files, pattern + '.*')
    matching_files.sort()

    for basename in matching_subdirs + matching_files:
      yield join(current_dir, basename)



# Exposed Storage API
LOCAL_STORE = Store(settings.DATA_DIRS)
STORE = Store(settings.DATA_DIRS, hosts=settings.CLUSTER_SERVERS)
