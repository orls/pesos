from socket import inet_ntoa
from struct import pack

from kazoo.client import KazooClient
from twitter.common.zookeeper.group.kazoo_group import KazooGroup

from .vendor.mesos.mesos_pb2 import MasterInfo


class MasterDetector(object):
  @classmethod
  def from_url(cls, url):
    pass

  def detect(self, previous=None):
    pass


class StandaloneMasterDetector(MasterDetector):
  @classmethod
  def master_info_from_pid(cls, pid):
    pass

  def __init__(self, leader):
    # TODO(tarnfeld): Resolve to an IP address as hostnames cause issues
    # with libprocess PIDs
    self.leader = leader

  def appoint(self, leader):
    pass

  def detect(self, previous=None):
    return self.leader


class MesosKazooGroup(KazooGroup):
  """
  Mesos uses 'info_' as a prefix for member nodes.
  """
  MEMBER_PREFIX = 'info_'


class ZookeeperMasterDetector(MasterDetector):
  """
  Zookeeper based master detector.
  """

  def __init__(self, url, election_path='/'):
    # strip off "zk://" prefix and pass to kazoo
    url = url[len('zk://'):]
    self._kazoo_client = KazooClient(url)
    # initialize group watcher once - even if we don't use callback interface
    # it makes a lot movements in it's __init__
    self._group_watcher = MesosKazooGroup(self._kazoo_client, election_path)
    self._previous = frozenset()  # previously detected group members
    # let's start async because there a lot of other scenarios
    # in which we fail to detect master even after connection
    # and even connection can fail anytime
    self._kazoo_client.start_async()

  def detect(self, previous=None):
    while True:  # seems like we can receive empty set
      previous = self._group_watcher.monitor(self._previous)
      if previous:
        break
    # we receive a set of members, we need master, which is the first one
    self._previous = previous
    leader = sorted(self._previous)[0]
    leader_data = self._group_watcher.info(leader)
    master_info = MasterInfo()
    master_info.MergeFromString(leader_data)
    return "{ip}:{port}".format(ip=inet_ntoa(pack('<L', master_info.ip)),
                                port=master_info.port)
