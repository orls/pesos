import threading
import socket
from struct import pack
from urlparse import urlparse
from concurrent.futures import Future

from kazoo.client import KazooClient
from twitter.common.zookeeper.group.kazoo_group import KazooGroup

from .vendor.mesos.mesos_pb2 import MasterInfo


class MasterDetector(object):
  @classmethod
  def from_url(cls, url):
    raise NotImplementedError

  def appoint(self, leader):
    pass

  def detect(self, previous=None):
    """Detect a master should it change from ``previous``.

    :keyword previous: Either ``None`` or a :class:`Process` object pointing to the previously
      detected master.
    :returns: :class:`Future` that can either provide ``None`` or a :class:`Process` value.
    """
    future = Future()
    future.set_exception(NotImplementedError())
    return future


class FutureMasterDetector(MasterDetector):
  """A base class for future-based master detectors."""

  def __init__(self):
    self.__lock = threading.Lock()
    self._leader = None
    self._future_queue = []  # list of queued detection requests

  def fail(self, exception):
    with self.__lock:
      future_queue, self._future_queue = self._future_queue, []
      for future in future_queue:
        future.set_exception(exception)

  def appoint(self, leader):
    with self.__lock:
      if leader == self._leader:
        return
      self._leader = leader
      future_queue, self._future_queue = self._future_queue, []
      for future in future_queue:
        future.set_result(leader)

  def detect(self, previous=None):
    with self.__lock:
      future = Future()
      if previous != self._leader:
        future.set_result(self._leader)
        return future
      self._future_queue.append(future)
      future.set_running_or_notify_cancel()
      return future


class StandaloneMasterDetector(FutureMasterDetector):
  """A concrete implementation of a standalone master detector."""

  def __init__(self, leader=None):
    super(StandaloneMasterDetector, self).__init__()

    try:
      host, port = leader.split(":")
    except ValueError:
      raise Exception("Expected host:port but got %s" % leader)

    # Resolve the host to ensure we have an IP address
    host = socket.gethostbyname(host)
    self.appoint("%s:%s" % (host, port))


class MesosKazooGroup(KazooGroup):
  """
  Mesos uses 'info_' as a prefix for member nodes.
  """

  MEMBER_PREFIX = 'info_'


class ZookeeperMasterDetector(MasterDetector):
  """
  Zookeeper based Mesos master detector.
  """

  ZK_PREFIX = "zk://"

  def __init__(self, url):
    url = urlparse(url)
    if url.scheme.lower() != "zk":
      raise Exception("Expected zk:// URL")

    self._previous = frozenset()  # Previously detected group members
    self._kazoo_client = KazooClient(url.netloc)

    # Initialize group watcher once - even if we don't use callback interface
    # it makes a lot movements in it's __init__
    self._group_watcher = MesosKazooGroup(self._kazoo_client, url.path)

    # Let's start async because there a lot of other scenarios
    # in which we fail to detect master even after connection
    # and even connection can fail anytime
    self._kazoo_client.start_async()

  def detect(self, previous=None):
    # Sit and wait until we detect nodes in the group
    while True:
      previous = self._group_watcher.monitor(self._previous)
      if previous:  # seems like we can receive empty set so let's be sure
        break

    # We receive a set of members, we need master, which is the first one
    self._previous = previous

    leader = sorted(self._previous)[0]
    leader_data = self._group_watcher.info(leader)

    master_info = MasterInfo()
    master_info.MergeFromString(leader_data)
    leader = "{ip}:{port}".format(
        ip=socket.inet_ntoa(pack('<L', master_info.ip)),
        port=master_info.port
    )

    future = Future()
    future.set_result(leader)
    return future
