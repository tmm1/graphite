from graphite.intervals import IntervalSet, Interval


class Node:
  name = property(lambda self: self.path.split('.')[-1])


class BranchNode(Node):
  is_leaf = False

  def __init__(self, path):
    self.path = path


class LeafNode(Node):
  is_leaf = True

  def __init__(self, path, reader):
    self.path = path
    self.reader = reader
    self.intervals = reader.get_intervals()

  def fetch(self, startTime, endTime):
    return self.reader.fetch(startTime, endTime)
