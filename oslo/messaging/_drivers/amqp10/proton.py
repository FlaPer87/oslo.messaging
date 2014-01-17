#    Copyright 2014 OpenStack Foundation
#    Copyright 2014, Red Hat, Inc.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

import abc
import errno
import logging
import os
import select
import socket
import threading
import time
import uuid

from oslo.config import cfg

from six import moves

from oslo.messaging._drivers.amqp10 import proton_wrapper
from oslo.messaging._drivers import base
from oslo.messaging.openstack.common import importutils
from oslo.messaging.openstack.common import jsonutils

# FIXME(markmc): remove this
_ = lambda s: s

proton = importutils.try_import("proton")

LOG = logging.getLogger(__name__)

proton_opts = [
    cfg.StrOpt('server_request_prefix',
               default='',
               help="address prefix used when sending to a specific server"),
    cfg.StrOpt('broadcast_prefix',
               default='',
               help="address prefix used when broadcasting to all servers"),
    cfg.StrOpt('group_request_prefix',
               default='',
               help="address prefix when sending to any server in group"),

    cfg.StrOpt('gateway_hostname',
               default='localhost',
               help='hostname of next-hop'),
    cfg.IntOpt('gateway_port',
               default=5672,
               help='port for next-hop'),
    cfg.ListOpt('gateway_hosts',
                default=['$gateway_hostname:$gateway_port'],
                help='Set of available next-hops'),
    cfg.StrOpt('amqp10_username',
               default='',
               help='Username for next-hop connection'),
    cfg.StrOpt('amqp10_password',
               default='',
               help='Password for next-hop connection',
               secret=True),
    cfg.StrOpt('amqp10_sasl_mechanisms',
               default='',
               help='Space separated list of SASL mechanisms to use for auth'),
    cfg.IntOpt('amqp10_idle_timeout',
               default=0,
               help='Seconds before idle connections are dropped'),
    cfg.StrOpt('amqp10_container_name',
               default='',
               help='Name for the AMQP container'),

    # @todo: other options?

    # for now:
    cfg.BoolOpt('amqp10_point_to_point',
                default=False,
                help='Enable point-to-point messaging.'),

    cfg.BoolOpt('proton_trace',
                default=False,
                help='Enable protocol tracing')
]

JSON_CONTENT_TYPE = 'application/json; charset=utf8'


class _SocketConnection(proton_wrapper.ConnectionEventHandler):
    """Associates a proton Connection with a python network socket,
    and handles all connection-related events.
    """

    def __init__(self, name, socket, container,
                 properties, handler=None):
        self.name = name
        self.socket = socket
        self._properties = properties
        self.connection = container.create_connection(name,
                                                      handler or self,
                                                      properties)
        self.connection.user_context = self

    def fileno(self):
        """Allows use of a _SocketConnection in a select() call.
        """
        return self.socket.fileno()

    def read(self):
        """Called when socket is read-ready."""
        try:
            rc = proton_wrapper.sockets.read_socket_input(self.connection,
                                                          self.socket)
        except Exception:
            rc = proton_wrapper.Connection.EOS
        if rc > 0:
            self.connection.process(time.time())
        return rc

    def write(self):
        """Called when socket is write-ready."""
        try:
            rc = proton_wrapper.sockets.write_socket_output(self.connection,
                                                            self.socket)
        except Exception:
            rc = proton_wrapper.Connection.EOS
        if rc > 0:
            self.connection.process(time.time())
        return rc

    # ConnectionEventHandler callbacks
    def connection_active(self, connection):
        LOG.info("APP: CONN ACTIVE")

    def connection_closed(self, connection, reason):
        LOG.info("APP: CONN CLOSED")

    def sender_requested(self, connection, link_handle,
                         requested_source, properties):
        ## @todo: support needed for peer-2-peer
        LOG.info("APP: SENDER LINK REQUESTED")

    def receiver_requested(self, connection, link_handle,
                           requested_target, properties):
        ## @todo: support needed for peer-2-peer
        LOG.info("APP: RECEIVER LINK REQUESTED")

    # SASL callbacks:

    def sasl_step(self, connection, pn_sasl):
        LOG.info("SASL STEP")

    def sasl_done(self, connection, result):
        LOG.info("APP: SASL DONE: %s" % result)


class Requests(object):
    def __init__(self):
        self._requests = moves.queue.Queue()
        self._wakeup_pipe = os.pipe()

    def wakeup(self, request=None):
        if request:
            self._requests.put(request)
        os.write(self._wakeup_pipe[1], "!")

    def fileno(self):
        return self._wakeup_pipe[0]

    def read(self):
        os.read(self._wakeup_pipe[0], 512)
        while not self._requests.empty():
            self._requests.get()()


class ProcessingThread(threading.Thread):

    def __init__(self, container_name=None):
        threading.Thread.__init__(self)

        # handle requests from other threads
        self._requests = Requests()

        # Configure a container
        if container_name:
            self._container = proton_wrapper.Container(container_name)
        else:
            self._container = proton_wrapper.Container(uuid.uuid4().hex)

        self.name = "Thread for Proton container: %s" % self._container.name
        self._shutdown = False
        self.daemon = True
        self.start()

    def wakeup(self, request=None):
        """Wake up the processing thread."""
        self._requests.wakeup(request)

    def destroy(self):
        """Stop the processing thread, releasing all resources.
        """
        LOG.debug("Stopping Proton container %s" % self._container.name)
        self.wakeup(lambda: self._do_shutdown())
        self.join()

    def _do_shutdown(self):
        self._shutdown = True

    def connect(self, hostname, port=5672, properties={}, name=None,
                sasl_mechanisms="ANONYMOUS", handler=None):
        """Get a _SocketConnection to a peer represented by url."""
        key = name or "%s:%i" % (hostname, port)
        # return pre-existing
        conn = self._container.get_connection(key)
        if conn:
            assert isinstance(conn.user_context, _SocketConnection)
            return conn.user_context

        addr = socket.getaddrinfo(hostname, port,
                                  socket.AF_INET, socket.SOCK_STREAM)
        if not addr:
            error = _("Could not translate address '%s'") % key
            LOG.warn(error)
            raise Exception(error)
        my_socket = socket.socket(addr[0][0], addr[0][1], addr[0][2])
        my_socket.setblocking(0)  # 0=non-blocking
        try:
            my_socket.connect(addr[0][4])
        except socket.error as e:
            if e[0] != errno.EINPROGRESS:
                error = _("Socket connect failure '%s'") % str(e)
                LOG.warn(error)
                raise Exception(error)

        # create a new connection - this will be stored in the
        # container, using the specified name as the lookup key, or if
        # no name was provided, the host:port combination
        sc = _SocketConnection(key, my_socket,
                               self._container,
                               properties, handler=handler)
        if sasl_mechanisms:
                pn_sasl = sc.connection.sasl
                pn_sasl.mechanisms(sasl_mechanisms)
                # @todo KAG: server if accepting inbound connections
                pn_sasl.client()
        sc.connection.open()
        self.wakeup()
        return sc

    def run(self):
        """Run the proton event/timer loop."""
        LOG.debug(_("Starting Proton thread, container=%s"),
                  self._container.name)

        while not self._shutdown:
            readers, writers, timers = self._container.need_processing()

            readfds = [c.user_context for c in readers]
            # additionally, always check for readability of pipe we
            # are using to wakeup processing thread by other threads
            readfds.append(self._requests)
            writefds = [c.user_context for c in writers]

            timeout = None
            if timers:
                deadline = timers[0].next_tick  # 0 == next expiring timer
                now = time.time()
                timeout = 0 if deadline <= now else deadline - now

            LOG.debug(_("proton thread select() call (timeout=%s)"),
                      str(timeout))
            readable, writable, ignore = select.select(readfds,
                                                       writefds,
                                                       [],
                                                       timeout)
            LOG.debug(_("select() returned"))

            for r in readable:
                r.read()

            for t in timers:
                if t.next_tick > time.time():
                    break
                t.process(time.time())

            for w in writable:
                w.write()

        LOG.debug(_("Stopping Proton thread, container=%s"),
                  self._container.name)
        # abort any requests. Setting the lists to None here prevents further
        # requests from being created


class Replies(proton_wrapper.ReceiverEventHandler):
    def __init__(self, connection, on_ready):
        self._correlation = {}  # map of correlation-id to response queue
        self._ready = False
        self._on_ready = on_ready
        self._receiver = connection.create_receiver("replies",
                                                    eventHandler=self)
        self.capacity = 100  # somewhat arbitrary
        self._credit = 0

    def ready(self):
        return self._ready

    def prepare_for_response(self, request, reply_queue):
        assert(self._ready)
        if reply_queue:
            key = uuid.uuid4().hex
            # TODO(grs): need to synchronise concurrent access to
            # correlation table
            self._correlation[key] = reply_queue
            request.id = key
            request.reply_to = self._receiver.source_address

    def receiver_active(self, receiver_link):
        self._ready = True
        self._update_credit()
        self._on_ready()

    def receiver_remote_closed(self, receiver, error=None):
        # TODO(grs)
        LOG.error(_("Reply subscription closed by peer: %s"),
                  (error or "no error given"))

    def message_received(self, receiver, message, handle):
        self._credit = self._credit - 1
        self._update_credit()

        key = message.correlation_id
        # TODO(grs): need to synchronise concurrent access to
        # correlation table
        if key in self._correlation:
            LOG.debug("Received response for %s" % key)
            self._correlation[key].put(message)
            # cleanup (only have one response per request)
            del self._correlation[key]
        else:
            LOG.warn("Can't correlate %s (%s)" % (key, self._correlation))

    def _update_credit(self):
        if self.capacity > self._credit:
            self._receiver.add_capacity(self.capacity - self._credit)
            self._credit = self.capacity


class Server(proton_wrapper.ReceiverEventHandler):
    def __init__(self, connection, addresses, incoming):
        self._incoming = incoming
        self._receivers = []
        for a in addresses:
            r = connection.create_receiver(source_address=a,
                                           target_address=a,
                                           eventHandler=self)
            r.add_capacity(1)  # TODO(grs)
            self._receivers.append(r)

    def receiver_remote_closed(self, receiver, error=None):
        text = _("Server subscription %(addr)s closed by peer: %(err_msg)s")
        vals = {
            "addr": receiver.source_address or receiver.target_address,
            "err_msg": error or "no error given"
        }
        LOG.error(text % vals)

    def message_received(self, receiver, message, handle):
        receiver.add_capacity(1)
        self._incoming.put(message)


class ProtocolManager(proton_wrapper.ConnectionEventHandler):
    def __init__(self, host, port=5672):
        self.processor = ProcessingThread()
        self._tasks = moves.queue.Queue()
        self._senders = {}
        self._servers = {}
        # can't handle a request until the replies link is active, as
        # we need the peer assigned address, so need to delay any
        # processing of task queue until this is done
        self._replies = None
        self.processor.wakeup(lambda: self._do_connect(host, port))

    def _do_connect(self, host, port=5672):
        """Establish connection and reply subscripion on processor thread."""
        self._socket_connection = self.processor.connect(host, port,
                                                         handler=self)
        self._connection = self._socket_connection.connection
        LOG.debug("Connection initiated")

    def connection_active(self, connection):
        LOG.debug("Connection active, subscribing for replies")
        self._replies = Replies(self._connection, lambda: self._ready())

    def connection_closed(self, connection, reason):
        LOG.info("Connection closed")

    def _ready(self):
        LOG.debug("Reply subscription ready (%s)" % (self._tasks.empty()))
        if not self._tasks.empty():
            self.processor.wakeup(lambda: self._process_tasks())

    def _put_task(self, task):
        """Add task for execution on processor thread."""
        self._tasks.put(task)
        if self._replies and self._replies.ready():
            self.processor.wakeup(lambda: self._process_tasks())

    def _process_tasks(self):
        """Execute tasks on processor thread."""
        while not self._tasks.empty():
            try:
                self._tasks.get(False).execute(self)
            except Exception as e:
                LOG.error(_("Error processing task: %s"), str(e))

    def tasks(self):
        class Tasks(object):
            def __init__(self, mgr):
                self._mgr = mgr

            def put(self, task):
                self._mgr._put_task(task)
        return Tasks(self)

    def destroy(self):
        self.processor.destroy()

    def request(self, target, request, reply_queue=None):
        """Send a request to the given target, and arrange for a
        response to be put on the optional reply_queue if specified
        """
        address = self._resolve(target)
        self._replies.prepare_for_response(request, reply_queue)
        self._sender(address).send(request)

    def response(self, address, response):
        self._sender(address).send(response)

    def subscribe(self, target, requests):
        LOG.debug("Subscribing to %s" % target)
        assert(target.topic)
        assert(target.server)
        server_address = "%s.%s" % (target.topic, target.server)
        broadcast_address = "broadcast.%s" % target.topic
        addresses = [
            self._add_server_prefix(server_address),
            self._add_broadcast_prefix(broadcast_address),
            self._add_group_request_prefix(target.topic)
        ]
        self._servers[target] = Server(self._connection, addresses, requests)

    def _resolve(self, target):
        if target.server:
            address = "%s.%s" % (target.topic, target.server)
            address = self._add_server_prefix(address)
        elif target.fanout:
            address = "broadcast.%s" % target.topic
            address = self._add_broadcast_prefix(address)
        else:
            address = self._add_group_request_prefix(target.topic)
        return address

    def _sender(self, address):
        # if we already have a sender for that address, use it
        # else establish the sender and cache it
        if address in self._senders:
            sender = self._senders[address]
        else:
            sender = self._connection.create_sender(source_address=address,
                                                    target_address=address)
            self._senders[address] = sender
        return sender

    def _add_server_prefix(self, address):
        #TODO(grs)
        return "/queues/%s" % address

    def _add_broadcast_prefix(self, address):
        #TODO(grs)
        return "/topics/%s" % address

    def _add_group_request_prefix(self, address):
        #TODO(grs)
        return "/queues/%s" % address


class Task(object):
    @abc.abstractmethod
    def execute(self, manager):
        """Perform operation on the protocol manager (will be called
        on a thread of its choosing).
        """


class SendTask(Task):
    def __init__(self, target, request, reply_expected):
        self._target = target
        self._request = request
        if reply_expected:
            self._reply_queue = moves.queue.Queue()

    def execute(self, manager):
        manager.request(self._target, self._request, self._reply_queue)

    def get_reply(self, timeout):
        if not self._reply_queue:
            return None
        return self._reply_queue.get(timeout)


class ListenTask(Task):
    def __init__(self, target, request_queue):
        self._target = target
        self._request_queue = request_queue

    def execute(self, manager):
        manager.subscribe(self._target, self._request_queue)


class ReplyTask(Task):
    def __init__(self, address, response, log_failure):
        self._address = address
        self._response = response
        self._log_failure = log_failure

    def execute(self, manager):
        manager.response(self._address, self._response)


def marshal_response(reply=None, failure=None):
    #TODO(grs): do replies have a context?
    msg = proton.Message()
    if failure:
        data = {"failure": failure}
    else:
        data = {"response": reply}
    msg.body = jsonutils.dumps(data)
    return msg


def unmarshal_response(message):
    #TODO(grs)
    data = jsonutils.loads(message.body)
    if "response" in data:
        return data["response"]
    elif "failure" in data:
        #???
        raise Exception(str(data))
    else:
        #???
        return data


def marshal_request(request, context, envelope):
    #TODO(grs)
    msg = proton.Message()
    data = {
        "request": request,
        "context": context
    }
    msg.body = jsonutils.dumps(data)
    return msg


def unmarshal_request(message):
    #TODO(grs)
    data = jsonutils.loads(message.body)
    if "request" in data:
        request = data["request"]
    if "context" in data:
        context = data["context"]
    return (request, context)


class ProtonIncomingMessage(base.IncomingMessage):
    def __init__(self, listener, ctxt, request, task_queue, message):
        base.IncomingMessage.__init__(self, listener, ctxt, request)
        self._task_queue = task_queue
        self._reply_to = message.reply_to
        self._correlation_id = message.id

    def reply(self, reply=None, failure=None, log_failure=True):
        response = marshal_response(reply=reply, failure=failure)
        if self._correlation_id:
            response.correlation_id = self._correlation_id
        self._task_queue.put(ReplyTask(self._reply_to, response, log_failure))


class ProtonListener(base.Listener):
    def __init__(self, driver, target, tasks):
        base.Listener.__init__(self, driver, target)
        self._tasks = tasks
        self._incoming = moves.queue.Queue()

    def poll(self):
        message = self._incoming.get()
        request, ctxt = unmarshal_request(message)
        return ProtonIncomingMessage(self, ctxt, request, self._tasks, message)

    def incoming(self):
        return self._incoming


class ProtonDriver(base.BaseDriver):

    def __init__(self, conf, url,
                 default_exchange=None, allowed_remote_exmods=[]):
        base.BaseDriver.__init__(self, conf, url, default_exchange,
                                 allowed_remote_exmods)
        # TODO(grs): handle reconnect, authentication etc
        hostname = url.hosts[0].hostname or "localhost"
        port = url.hosts[0].port or 5672
        self._mgr = ProtocolManager(hostname, port)

    def send(self, target, ctxt, message,
             wait_for_reply=None, timeout=None, envelope=False):
        """Send a message to the given target."""
        request = marshal_request(message, ctxt, envelope)
        if timeout:
            request.ttl = timeout
        task = SendTask(target, request, wait_for_reply)
        self._mgr.tasks().put(task)
        reply = task.get_reply(timeout)
        if reply:
            return unmarshal_response(reply)
        else:
            return None

    def send_notification(self, target, ctxt, message, version):
        """Send a notification message to the given target."""

    def listen(self, target):
        """Construct a Listener for the given target."""
        LOG.debug("Listen to %s" % target)
        listener = ProtonListener(self, target, self._mgr.tasks())
        self._mgr.tasks().put(ListenTask(target, listener._incoming))
        return listener

    def cleanup(self):
        """Release all resources."""
        LOG.info("Cleaning up ProtonDriver")
        self._mgr.destroy()
