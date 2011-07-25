#!/usr/bin/env python

import sys, os
import whisper
from optparse import OptionParser

UnitMultipliers = {
  's' : 1,
  'm' : 60,
  'h' : 60 * 60,
  'd' : 60 * 60 * 24,
  'y' : 60 * 60 * 24 * 365,
}


def parseRetentionDef(retentionDef):
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

option_parser = OptionParser(usage='''%prog path secondsPerPoint:pointsToStore [secondsPerPoint:pointsToStore]* ''')
option_parser.add_option('--xFilesFactor', default=0.5, type='float')
option_parser.add_option('--overwrite', default=False, action='store_true')

(options, args) = option_parser.parse_args()

if len(args) < 2:
  option_parser.print_usage()
  sys.exit(1)

path = args[0]
archives = [ parseRetentionDef(retentionDef) for retentionDef in args[1:] ]

if options.overwrite and os.path.exists(path):
  print 'Overwriting existing file: %s' % path
  os.unlink(path)

whisper.create(path, archives, xFilesFactor=options.xFilesFactor)

size = os.stat(path).st_size
print 'Created: %s (%d bytes)' % (path,size)
