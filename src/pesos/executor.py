import functools
import logging
import threading
import uuid

from .vendor import mesos
from .util import timed

from compactor.context import Context
from compactor.process import ProtobufProcess


log = logging.getLogger(__name__)


class ShutdownProcess(Process):
  pass


class ExecutorProcess(ProtobufProcess):
  class Error(Exception): pass

  def __init__(self,
               slave_pid,
               driver,
               executor,
               slave_id,
               framework_id,
               executor_id,
               directory,
               checkpoint,
               recovery_timeout):

    self.slave = slave_pid
    self.driver = driver
    self.executor = executor

    self.slave_id = slave_id
    self.framework_id = framework_id
    self.executor_id = executor_id

    self.aborted = threading.Event()
    self.connected = threading.Event()

    self.directory = directory
    self.checkpoint = checkpoint
    self.recovery_timeout = recovery_timeout

    self.updates = {}  # unacknowledged updates
    self.tasks = {}  # unacknowledged tasks

  def ignore_if_aborted(method):
    @functools.wraps(method)
    def _wrapper(self, *args, **kwargs):
      if self.aborted.is_set():
        log.info('Ignoring message from slave %s because the driver is aborted.' % self.slave_id)
        return
      return method(self, *args, **kwargs)
    return _wrapper

  @ProtobufProcess.install(mesos.internal.ExecutorRegisteredMessage)
  @ignore_if_aborted
  def registered(self, from_pid, message):
    if self.aborted.is_set():
      log.info('Ignoring registered message from slave %s because the driver is aborted.'
          % message.slave_id)
      return

    log.info('Executor registered on slave %s' % slave_id)
    self.connected.set()
    self.connection = uuid.uuid4()

    with timed(log.debug, 'executor::registered'):
      self.executor.registered(
          self.driver, message.executor_info, message.framework_info, message.slave_info)

  @ProtobufProcess.install(mesos.internal.ExecutorReregisteredMessage)
  @ignore_if_aborted
  def reregistered(self, from_pid, message):
    # TODO(wickman) Should we be validating that self.slave_id == message.slave_id?
    log.info('Executor re-registered on slave %s' % self.slave_id)
    self.connected.set()
    self.connection = uuid.uuid4()

    with timed(log.debug, 'executor::reregistered'):
      self.executor.reregistered(self.driver, message.slave_info)

  @ProtobufProcess.install(mesos.internal.ReconnectExecutorMessage)
  @ignore_if_aborted
  def reconnect(self, from_pid, message):
    log.info('Received reconnect request from slave %s' % slave_id)
    self.slave = from_pid
    self.link(from_pid)

    reregister_message = mesos.internal.ReregisterExecutorMessage()
    reregister_message.executor_id = self.executor_id
    reregister_message.framework_id = self.framework_id
    reregister_message.updates = list(self.updates.values())
    reregister_message.tasks = list(self.tasks.values())
    self.send(self.slave, reregister_message)

  @ProtobufProcess.install(mesos.internal.RunTaskMessage)
  @ignore_if_aborted
  def run_task(self, from_pid, message):
    task = message.task

    if task.task_id in self.tasks:
      raise self.Error('Unexpected duplicate task %s' % task)

    self.tasks[task.task_id] = task

    with timed(log.debug, 'executor::launch_task'):
      self.executor.launch_task(self.driver, task)

  @ProtobufProcess.install(mesos.internal.KillTaskMessage)
  @ignore_if_aborted
  def kill_task(self, from_pid, message):
    log.info('Executor asked to kill task %s' % message.task_id)

    with timed(log.debug, 'executor::kill_task'):
      self.executor.kill_task(self.driver, message.task_id)

  @ProtobufProcess.install(mesos.internal.StatusUpdateAcknowledgementMessage)
  @ignore_if_aborted
  def status_update_acknowledgement(self, from_pid, message):
    uuid = uuid.UUID(bytes=message.uuid)

    log.info('Executor received status update acknowledgement %s for task %s of framework %s' % (
        uuid, message.task_id, message.framework_id)

    if not self.updates.pop(uuid, None):
      log.error('Unknown status update %s!' % uuid)

    if not self.tasks.pop(message.task_id, None):
      log.error('Unknown task %s!' % message.task_id)

  @ProtobufProcess.install(mesos.internal.FrameworkToExecutorMessage)
  @ignore_if_aborted
  def framework_message(self, from_pid, message):
    log.info('Executor received framework message')

    with timed(log.debug, 'executor::framework_message'):
      self.executor.framework_message(self.driver, message.data)

  @ProtobufProcess.install(mesos.internal.ShutdownExecutorMessage)
  @ignore_if_aborted
  def shutdown(self, from_pid, message):
    self.context.spawn(ShutdownProcess())

    with timed(log.debug, 'executor::shutdown'):
      self.executor.shutdown(self.driver)

    self.aborted.set()

  def stop(self):
    self.terminate()

  @ignore_if_aborted
  def exited(self, pid):
    if self.checkpoint and self.connected.is_set():
      self.connected.clear()
      log.info('Slave exited but framework has checkpointing enabled.')
      log.info('Waiting %s to reconnect with %s' % (self.recovery_timeout, self.slave_id))
      self.context.delay(self.recovery_timeout, self.pid, '_recovery_timeout', self.connection)
      return

    log.info('Slave exited.  Shutting down.')
    self.connected.clear()
    self.context.spawn(ShutdownProcess())

    with timed(log.debug, 'executor::shutdown'):
      self.executor.shutdown(self.driver)

    self.aborted.set()

  def _recovery_timeout(self, connection):
    if self.connected.is_set():
      return

    if self.connection == connection:
      log.info('Recovery timeout exceeded, shutting down.')
      self.shutdown()

  def send_status_update(self, status):
    if self.status.state is mesos.TASK_STAGING:
      log.error('Executor is not allowed to send TASK_STAGING, aborting!')
      self.driver.abort()
      with timed(log.debug, 'executor::error'):
        self.executor.error(self.driver, 'Attempted to send TASK_STAGING status update.')
      return

    update = mesos.internal.StatusUpdate()
    update.framework_id = self.framework_id
    update.executor_id = self.executor_id
    update.slave_id = self.slave_id
    update.status = status
    update.timestamp = time.time()
    update.status.timestamp = update.timestamp
    update.status.slave_id = self.slave_id
    update.uuid = uuid.uuid4().get_bytes()
    message = mesos.internal.StatusUpdateMessage(update=update, pid=self.pid)
    self.updates[update.uuid] = update
    self.send(self.slave, message)

  def send_framework_message(self, data):
    message = mesos.internal.ExecutorToFrameworkMessage()
    message.slave_id = self.slave_id
    message.framework_id = self.framework_id
    message.executor_id = self.executor_id
    message.data = data
    self.send(self.slave, message)

  sendStatusUpdate = send_status_update
  sendFrameworkMessage = send_framework_message


class Executor(object):
  def registered(self, driver, executor_info, framework_info, slave_info):
    pass

  def reregistered(self, driver, slave_info):
    pass

  def disconnected(self, driver):
    pass

  def launch_task(self, driver, task):
    pass

  def kill_task(self, driver, task_id):
    pass

  def framework_message(self, data):
    pass

  def shutdown(self, driver):
    pass

  def error(self, driver, message):
    pass

  launchTask = launch_task
  killTask = kill_task
  frameworkMessage = framework_message


class ExecutorDriver(object):
  def start(self):
    pass

  def stop(self):
    pass

  def abort(self):
    pass

  def join(self):
    pass

  def run(self):
    pass

  def send_status_update(self, status):
    pass

  def send_framework_message(self, data):
    pass

  sendStatusUpdate = send_status_update
  sendFrameworkMessage = send_framework_message


class MesosExecutorDriver(ExecutorDriver):
  @classmethod
  def get_or_else(cls, key):
    try:
      return os.environ[key]
    except KeyError:
      raise RuntimeError('%s must be specified in order for the driver to work!' % key)

  @classmethod
  def get_bool(cls, key):
    if key not in os.environ:
      return False
    return os.environ[key] == "1"

  def __init__(self, executor, context=None):
    self.context = context or Context()
    self.executor = executor
    self.executor_process = None
    self.executor_pid = None
    self.status = mesos.DRIVER_NOT_STARTED
    self.lock = threading.Condition()

  def locked(method):
    @functools.wraps(method)
    def _wrapper(self, *args, **kw):
      with self.locked:
        return method(self, *args, **kw)
    return _wrapper

  @locked
  def start(self):
    local = self.get_bool('MESOS_LOCAL')
    slave_pid = PID.from_string(self.get_or_else('MESOS_SLAVE_PID'))
    slave_id = self.get_or_else('MESOS_SLAVE_ID')
    framework_id = self.get_or_else('MESOS_FRAMEWORK_ID')
    executor_id = self.get_or_else('MESOS_EXECUTOR_ID')
    directory = self.get_or_else('MESOS_DIRECTORY')
    checkpoint = self.get_bool('MESOS_CHECKPOINT')
    recovery_timeout_secs = 15 * 60  # 15 minutes
    if checkpoint:
      # TODO(wickman) Implement Duration.  Instead take seconds for now
      try:
        recovery_timeout = int(self.get_or_else('MESOS_RECOVERY_TIMEOUT'))
      except ValueError:
        raise RuntimeException('MESOS_RECOVERY_TIMEOUT must be in seconds.')

    assert self.executor_process is None
    self.executor_process = ExecutorProcess(
        slave_pid,
        self,
        self.executor,
        slave_id,
        framework_id,
        executor_id,
        directory,
        checkpoint,
        recovery_timeout
    )
    self.executor_pid = self.context.spawn(self.executor_process)
    self.status = mesos.DRIVER_RUNNING
    return self.status

  @locked
  def stop(self):
    if self.status not in (mesos.DRIVER_RUNNING, mesos.DRIVER_ABORTED):
      return self.status
    assert self.executor_process is not None
    self.context.dispatch(self.executor_pid, 'stop')
    aborted = self.status == mesos.DRIVER_ABORTED
    self.status = mesos.DRIVER_STOPPED
    return mesos.DRIVER_ABORTED if aborted else self.status

  @locked
  def abort(self):
    if self.status is not mesos.DRIVER_RUNNING:
      return self.status
    self.executor_process.aborted.set()
    self.context.dispatch(self.executor_pid, 'abort')
    self.status = mesos.DRIVER_ABORTED
    return self.status

  @locked
  def join(self):
    if self.status is not mesos.DRIVER_RUNNING:
      return self.status

    while self.status is mesos.DRIVER_RUNNING:
      self.lock.wait()

    assert self.status in (mesos.DRIVER_ABORTED, mesos.DRIVER_STOPPED)
    return self.status

  def run(self):
    self.status = self.start()
    if self.status is not mesos.DRIVER_RUNNING:
      return self.status
    else:
      return self.join()

  @locked
  def send_status_update(self, status):
    if self.status is not mesos.DRIVER_RUNNING:
      return self.status
    assert self.executor_process is not None
    self.context.dispatch(self.executor_pid, 'send_status_update', status)
    return self.status

  @locked
  def send_framework_message(self, data):
    if self.status is not mesos.DRIVER_RUNNING:
      return self.status
    assert self.executor_process is not None
    self.context.dispatch(self.executor_pid, 'send_framework_message', data)
    return self.status
