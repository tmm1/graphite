import sys
import os
import pwd
from twisted.internet.protocol import Factory
from twisted.internet.error import ConnectionDone
from twisted.internet import reactor
from carbon import log


def daemonize():
  if os.fork() > 0: sys.exit(0)
  os.setsid()
  if os.fork() > 0: sys.exit(0)
  si = open('/dev/null', 'r')
  so = open('/dev/null', 'a+')
  se = open('/dev/null', 'a+', 0)
  os.dup2(si.fileno(), sys.stdin.fileno())
  os.dup2(so.fileno(), sys.stdout.fileno())
  os.dup2(se.fileno(), sys.stderr.fileno())


def dropprivs(user):
  uid,gid = pwd.getpwnam(user)[2:4]
  os.setregid(gid,gid)
  os.setreuid(uid,uid)
  return (uid,gid)


class LoggingMixin:
  def connectionMade(self):
    self.peer = self.transport.getPeer()
    self.peerAddr = "%s:%d" % (self.peer.host, self.peer.port)
    log.listener("%s connection with %s established" % (self.__class__.__name__, self.peerAddr))

  def connectionLost(self, reason):
    if reason.check(ConnectionDone):
      log.listener("%s connection with %s closed cleanly" % (self.__class__.__name__, self.peerAddr))
    else:
      log.listener("%s connection with %s lost: %s" % (self.__class__.__name__, self.peerAddr, reason.value))


def startListener(interface, port, protocol):
  factory = Factory()
  factory.protocol = protocol
  return reactor.listenTCP( int(port), factory, interface=interface )
