from graphite.intervals import IntervalSet


class Node:
  name = property(lambda self: self.metric_path.split('.')[-1])
  has_data = False
  has_children = False
  intervals = IntervalSet([])


class BranchNode(Node):
  has_children = True

  def __init__(self, metric_path):
    self.metric_path = metric_path


class LeafNode(Node):
  has_data = True

  def __init__(self, metric_path, real_metric_path, reader):
    self.metric_path = metric_path
    self.real_metric_path = real_metric_path
    self.reader = reader

  def fetch(self, startTime, endTime):
    return self.reader.fetch(startTime, endTime)
