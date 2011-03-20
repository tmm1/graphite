

class Node:
  name = property(lambda self: self.path.split('.')[-1])
  local = True

  def __repr__(self):
    return '<%s[%x]: %s>' % (self.__class__.__name__, id(self), self.path)


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

  def __repr__(self):
    return '<LeafNode[%x]: %s (%s)>' % (id(self), self.path, self.reader)
