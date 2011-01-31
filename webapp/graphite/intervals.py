INFINITY = float('inf')
NEGATIVE_INFINITY = -INFINITY


class IntervalSet:
  def __init__(self, intervals):
    self.intervals = union_overlapping(intervals)
    self.size = sum(i.size for i in self.intervals)

  def __nonzero__(self):
    return self.size != 0

  def __sub__(self, other):
    return self.intersect( other.complement() )

  def complement(self):
    complementary = set()
    ordered = sorted(self.intervals, key=lambda i: i.start)
    cursor = NEGATIVE_INFINITY

    for interval in ordered:
      if cursor < interval.start:
        complementary.add( Interval(cursor, interval.start) )
        cursor = interval.end

    if cursor < INFINITY:
      complementary.add( Interval(cursor, INFINITY) )

    return IntervalSet(complementary)

  def intersect(self, other):
    intersections = [i.intersect(j) for i in self.intervals
                                    for j in other.intervals
                                    if i.intersect(j)]
    return IntervalSet(intersections)

  def intersect_interval(self, interval):
    intersections = [ i.intersect(interval) for i in self.intervals 
                                            if i.intersect(interval) ]
    return IntervalSet(intersections)

  def union(self, other):
    return IntervalSet(self.intervals | other.intervals)

  def subset_of(self, other):
    for myInterval in self.intervals:
      subset_of_other = False

      for theirInterval in other.intervals:
        if myInterval.subset_of(theirInterval):
          subset_of_other = True
          break

      if not subset_of_other:
        return False

    return True



class Interval:
  def __init__(self, start, end):
    if end - start < 0:
      raise ValueError("Invalid interval start=%s end=%s" % (start, end))

    self.start = start
    self.end = end
    self.tuple = (start, end)
    self.size = self.end - self.start

  def __eq__(self, other):
    return self.tuple == other.tuple

  def __hash__(self):
    return hash( self.tuple )

  def __len__(self):
    raise TypeError("len() doesn't support infinite values, use the 'size' attribute instead")

  def __nonzero__(self):
    return self.size != 0

  def __repr__(self):
    return '<Interval: %s>' % str(self.tuple)

  def intersect(self, other):
    start = max(self.start, other.start)
    end = min(self.end, other.end)

    if end > start:
      return Interval(start, end)

  def overlaps(self, other):
    earlier = self if self.start <= other.start else other
    later = self if earlier is other else other
    return earlier.end >= later.start

  def union(self, other):
    if not self.overlaps(other):
      raise TypeError("Union of disjoint intervals is not an interval")

    start = min(self.start, other.start)
    end = max(self.end, other.end)
    return Interval(start, end)

  def subset_of(self, other):
    return (self.start >= other.start and self.end <= other.end)

  def spanning_to(self, other):
    start = min(self.start, other.start)
    end = max(self.end, other.end)
    return Interval(start, end)



def union_overlapping(intervals):
  """Union any overlapping intervals in the given set."""
  intervals = set(intervals)
  disjoint_intervals = set()

  while intervals:
    selected = intervals.pop()
    selected_is_disjoint = True

    for other in intervals:
      if selected.overlaps(other):
        intervals.remove(other)
        intervals.add( selected.union(other) )
        selected_is_disjoint = False
        break

    if selected_is_disjoint:
      disjoint_intervals.add(selected)

  return disjoint_intervals
