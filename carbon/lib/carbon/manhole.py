from twisted.cred import portal, checkers
from twisted.conch.manhole import Manhole
from twisted.conch.manhole_ssh import TerminalRealm, ConchFactory
from twisted.internet import reactor
from carbon.conf import settings


# module state globals
namespace = {}


def start():
  sshRealm = TerminalRealm()
  sshRealm.chainedProtocolFactory.protocolFactory = lambda _: Manhole(namespace)
  #TODO replace this with a better credential checker
  credChecker = checkers.InMemoryUsernamePasswordDatabaseDontUse(carbon='')
  sshPortal = portal.Portal(sshRealm)
  sshPortal.registerChecker(credChecker)
  sessionFactory = ConchFactory(sshPortal)
  reactor.listenTCP(settings.MANHOLE_PORT, sessionFactory, interface=settings.MANHOLE_INTERFACE)
