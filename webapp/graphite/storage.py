import os
import time
import fnmatch
import socket
import errno
from os.path import islink, isdir, isfile, realpath, join, dirname, basename
from django.conf import settings
from ceres import CeresNode
from graphite.logger import log
from graphite.remote_storage import RemoteStore
from graphite.node import BranchNode, LeafNode
from graphite.intervals import Interval, IntervalSet
from graphite.readers import (MultiReader, CeresReader, WhisperReader,
                              GzippedWhisperReader, RRDReader)


class Store:
  def __init__(self, directories=[], hosts=[]):
    self.directories = directories
    remote_hosts = [host for host in hosts if not is_local_interface(host)]
    self.remote_stores = [ RemoteStore(host) for host in remote_hosts ]

    if not (directories or remote_hosts):
      raise ValueError("directories and remote_hosts cannot both be empty")


  def find(self, pattern, startTime=None, endTime=None):
    query = FindQuery(pattern, startTime, endTime)

    # Start remote searches
    remote_requests = [ r.find(query) for r in self.remote_stores if r.available ]
    matching_nodes = set()

    # Search locally
    for directory in self.directories:
      for node in find_nodes(directory, query):
        log.info("find() :: local :: %s" % node.path)
        matching_nodes.add(node)

    # Gather remote search results
    for request in remote_requests:
      for node in request.get_results():
        log.info("find() :: remote :: %s" % node.path)
        matching_nodes.add(node)

    # Group matching nodes by their path
    nodes_by_path = {}
    for node in matching_nodes:
      log.info("matching node: %s" % node)
      if node.path not in nodes_by_path:
        nodes_by_path[node.path] = []

      nodes_by_path[node.path].append(node)

    # Reduce matching nodes for each path to a minimal set
    for path, nodes in nodes_by_path.iteritems():
      log.info("path=%s  nodes=%s" % (path, str(nodes)))
      leaf_nodes = []

      # First we dispense with the BranchNodes
      for node in nodes:
        if node.is_leaf:
          leaf_nodes.append(node)
        else: #TODO need to filter branch nodes based on requested interval... how?!?!?
          yield node

      if not leaf_nodes:
        continue

      minimal_node_set = set()
      covered_intervals = IntervalSet([])

      def measure_of_added_coverage(node):
        relevant_intervals = node.intervals.intersect_interval(query.interval)
        relevant_intervals -= covered_intervals
        return relevant_intervals.size

      nodes_remaining = list(leaf_nodes)
      while nodes_remaining:
        best_node = max(nodes_remaining, key=measure_of_added_coverage)

        if measure_of_added_coverage(best_node) == 0:
          break

        nodes_remaining.remove(best_node)
        minimal_node_set.add(node)
        covered_intervals = covered_intervals.union(node.intervals)

      # Sometimes the requested interval falls within the caching window.
      # We include the most likely node if the gap is within tolerance.
      if not minimal_node_set:
        def distance_to_requested_interval(node):
          latest = sorted(node.intervals, key=lambda i: i.end)[-1]
          distance = query.interval.start - latest.end
          return distance if distance >= 0 else float('inf')

        best_candidate = min(leaf_nodes, key=distance_to_requested_interval)
        if distance_to_requested_interval(best_candidate) <= settings.FIND_TOLERANCE:
          minimal_node_set.add(best_candidate)

      reader = MultiReader(minimal_node_set)
      yield LeafNode(path, reader)



class FindQuery:
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

    return '<FindQuery: %s from %s until %s>' % (self.pattern, startString, endString)



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



DATASOURCE_DELIMETER = '::RRD_DATASOURCE::'

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
        ceres_node = CeresNode.fromFilesystemPath(absolute_path)

        if ceres_node.hasDataForInterval(query.startTime, query.endTime):
          reader = CeresReader(ceres_node, real_metric_path)
          yield LeafNode(metric_path, reader)

      else:
        yield BranchNode(metric_path)

    elif isfile(absolute_path):
      if absolute_path.endswith('.wsp') and WhisperReader.supported:
        reader = WhisperReader(absolute_path)
        yield LeafNode(metric_path, reader)

      elif absolute_path.endswith('.wsp.gz') and GzippedWhisperReader.supported:
        reader = GzippedWhisperReader(absolute_path)
        yield LeafNode(metric_path, reader)

      elif absolute_path.endswith('.rrd') and RRDReader.supported:
        if datasource_pattern is None:
          yield BranchNode(metric_path)

        else:
          for datasource_name in RRDReader.get_datasources(absolute_path):
            if fnmatch.fnmatch(datasource_name, datasource_pattern):
              reader = RRDReader(absolute_path, datasource_name)
              yield LeafNode(metric_path, reader)


def _find_paths(current_dir, patterns):
  """Recursively generates absolute paths whose components underneath current_dir
  match the corresponding pattern in patterns"""
  pattern = patterns[0]
  patterns = patterns[1:]
  entries = os.listdir(current_dir)

  subdirs = [e for e in entries if isdir( join(current_dir,e) )]
  matching_subdirs = fnmatch.filter(subdirs, pattern)
  matching_subdirs.sort()

  if len(patterns) == 1 and RRDReader.supported: #the last pattern may apply to RRD data sources
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
