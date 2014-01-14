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

import functools
import itertools
import logging
import time
import os

import eventlet
import greenlet
from oslo.config import cfg

from six import moves

from oslo.messaging._drivers import base
from oslo.messaging._drivers.amqp10 import proton_wrapper

#from oslo.messaging._drivers import amqp as rpc_amqp
#from oslo.messaging._drivers import amqpdriver
#from oslo.messaging._drivers import common as rpc_common
#from oslo.messaging.openstack.common import excutils
#from oslo.messaging.openstack.common import importutils
#from oslo.messaging.openstack.common import jsonutils

# FIXME(markmc): remove this
_ = lambda s: s

proton = importutils.try_import("proton")

LOG = logging.getLogger(__name__)

proton_opts = [
    cfg.StrOpt('server_request_prefix',
               default='',
               help="address prefix used when sending to a specific server")
    cfg.StrOpt('broadcast_prefix',
               default='',
               help="address prefix used when broadcasting to all servers")
    cfg.StrOpt('group_request_prefix',
               default='',
               help="address prefix used when load-balancing across a group of servers")

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
                help='Enable point-to-point messaging.')

    cfg.BoolOpt('proton_trace',
                default=False,
                help='Enable protocol tracing')
]

JSON_CONTENT_TYPE = 'application/json; charset=utf8'


class _SocketConnection(proton_wrapper.ConnectionEventHandler):
    """Associates a proton Connection with a python network socket, 
    and handles all connection-related events.
    """

    def __init__(self, name, socket, container, proton_lock,
                 properties):
        self.name = name
        self.socket = socket
        self._proton_lock = proton_lock
        self._properties = properties

        with proton_lock:
            self.connection = container.create_connection(name, self,
                                                          properties)
            self.connection.user_context = self

    def fileno(self):
        """Allows use of a _SocketConnection in a select() call.
        """
        return self.socket.fileno()

    def process_input(self):
        """Called when socket is read-ready"""
        try:
            rc = fusion.read_socket_input(self.connection,
                                          self.socket)
        except:
            rc = Connection.EOS
        if rc > 0:
            self.connection.process(time.time())
        return rc

    def send_output(self):
        """Called when socket is write-ready"""
        try:
            rc = fusion.write_socket_output(self.connection,
                                            self.socket)
        except:
            rc = Connection.EOS
        if rc > 0:
            self.connection.process(time.time())
        return rc

    # ConnectionEventHandler callbacks
    # Note well: all of these callbacks are invoked while the proton_lock is held

    def connection_active(self, connection):
        print "APP: CONN ACTIVE"

    def connection_closed(self, connection, reason):
        print "APP: CONN CLOSED"

    def sender_requested(self, connection, link_handle,
                         requested_source, properties):
        ## @todo: support needed for peer-2-peer
        print "APP: SENDER LINK REQUESTED"

    def receiver_requested(self, connection, link_handle,
                           requested_target, properties):
        ## @todo: support needed for peer-2-peer
        print "APP: RECEIVER LINK REQUESTED"

    # SASL callbacks:

    def sasl_step(self, connection, pn_sasl):
        print "SASL STEP"

    def sasl_done(self, connection, result):
        print "APP: SASL DONE"
        print result

class TaskQueue(Object):
    def __init__(self):
        self._wakeup_pipe = os.Pipe()
        self._tasks = moves.queue.Queue()

    def put(self, task):
        self._tasks.put(task)
        os.write(self._wakeup_pipe[1], "!")


class ContainerIO(threading.Thread):

    def __init__(self, container_name=None):
        if not proton:
            raise ImportError("Failed to import Qpid Proton library")

        threading.Thread.__init__(self)

        # queued tasks from other threads
        self._tasks = TaskQueue()

        #
        # Configure a container
        #
        if container_name:
            self._container = proton_wrapper.Container(container_name)
        else:
            self._container = proton_wrapper.Container(uuid.uuid4().hex)

        # run the Proton Engine event/timer thread:
        self._proton_lock = threading.Lock()

        self.name = "Thread for Proton container: %s" % self._container.name
        self._shutdown = False
        self.daemon = False # KAG: or True ???
        self.start()

    def wakeup(self):
        """Force the Proton thread to wake up"""
        os.write(self._tasks.pipe()[1], "!")

    def destroy(self):
        """Stop the Proton thread, releasing all resources.
        """
        LOG.debug("Stopping Proton container %s" % self._container.name)
        self._shutdown = True
        self.wakeup()
        self.join()

    def connect(self, hostname, port=5672, properties={}, name=None):
        """Get a _SocketConnection to a peer represented by url"""
        key = name or "%s:%i" % (host, port)
        # return pre-existing
        with self._proton_lock:
            conn = self._container.get_connection(key)
        if conn:
            assert isinstance(conn.user_context, _SocketConnection)
            return conn.user_context

        port = int(matches[2]) if matches[2] else None
        addr = socket.getaddrinfo(host, port, socket.AF_INET, socket.SOCK_STREAM)
        if not addr:
            error = "Could not translate address '%s'" % url
            LOG.warn(_(error))
            raise Exception(error)
        my_socket = socket.socket(addr[0][0], addr[0][1], addr[0][2])
        my_socket.setblocking(0) # 0=non-blocking
        try:
            my_socket.connect(addr[0][4])
        except socket.error, e:
            if e[0] != errno.EINPROGRESS:
                error = "Socket connect failure '%s'" % str(e)
                LOG.warn(_(error))
                raise Exception(error)

        # create a new connection - this will be stored in the
        # container, using the specified name as the lookup key, or if
        # no name was provided, the host:port combination
        sc = _SocketConnection(key, my_socket,
                               self._container,
                               self._proton_lock,
                               properties)
        if self.conf.amqp10_sasl_mechanisms:
                pn_sasl = sc.connection.sasl
                pn_sasl.mechanisms(self.conf.amqp10_sasl_mechanisms)
                # @todo KAG: server if accepting inbound connections
                pn_sasl.client()
        return sc


    def run(self):
        """Proton event/timer thread"""

        LOG.debug(_("Starting Proton thread container=%s") % self._container.name))

        while not self._shutdown:

            readfd = [self._tasks.pipe()[0]]
            writefd = []

            with self._proton_lock:
                readers,writers,timers = container.need_processing()

            # map fusion Connections back to my SocketConnections
            for c in readers:
                sc = c.user_context
                assert sc and isinstance(sc, _SocketConnection)
                readfd.append(sc)
            for c in writers:
                sc = c.user_context
                assert sc and isinstance(sc, _SocketConnection)
                writefd.append(sc)

            timeout = None
            if timers:
                deadline = timers[0].next_tick # 0 == next expiring timer
                now = time.time()
                timeout = 0 if deadline <= now else deadline - now

            LOG.debug(_("proton thread select() call (timeout=%s)" % str(timeout)))
            readable,writable,ignore = select.select(readfd,writefd,[],timeout)
            LOG.debug(_("select() returned"))

            while os.read(self._tasks.pipe()[0], 512):
                pass

            for r in readable:
                if r is self._tasks.pipe()[0]: continue
                assert isinstance(r, SocketConnection)

                # @todo: KAG update to use utility method
                with self._proton_lock:
                    count = r.connection.needs_input
                    if count > 0:
                        try:
                            sock_data = r.socket.recv(count)
                            if sock_data:
                                r.connection.process_input( sock_data )
                            else:
                                # closed?
                                r.connection.close_input()
                        except socket.timeout, e:
                            # I don't expect this
                            LOG.warn(_("Unexpected socket timeout %s" % str(e)))
                        except socket.error, e:
                            err = e.args[0]
                            # ignore non-fatal errors
                            if (err != errno.EAGAIN and
                                err != errno.EWOULDBLOCK and
                                err != errno.EINTR):
                                # otherwise, unrecoverable:
                                r.connection.close_input()
                                LOG.warn(_("Unexpected socket error %s" % str(e)))
                        except Exception, e:  # beats me...
                            r.connection.close_input()
                            LOG.warn(_("Unknown socket error" % str(e)))

                        r.connection.process(time.time())

            for t in timers:
                if t.next_tick > time.time():
                    break
                with self._proton_lock:
                    t.process(time.time())

            for w in writable:
                # @todo: KAG update to use utility method
                assert isinstance(w, SocketConnection)
                with self._proton_lock:
                    data = w.connection.output_data()
                    if data:
                        try:
                            rc = w.socket.send(data)
                            if rc > 0:
                                w.connection.output_written(rc)
                            else:
                                # else socket closed
                                w.connection.close_output()
                        except socket.timeout, e:
                            # I don't expect this
                            LOG.warn(_("Unexpected socket timeout %s" % str(e)))
                        except socket.error, e:
                            err = e.args[0]
                            # ignore non-fatal errors
                            if (err != errno.EAGAIN and
                                err != errno.EWOULDBLOCK and
                                err != errno.EINTR):
                                # otherwise, unrecoverable
                                w.connection.close_output()
                                LOG.warn(_("Unexpected socket error %s" % str(e)))
                        except Exception, e:  # beats me...
                            w.connection.close_output()
                            LOG.warn(_("Unknown socket error %s" % str(e)))
                        w.connection.process(time.time())

        LOG.debug(_("Stopping Proton thread, container=%s") % self._container.name))

        # abort any requests. Setting the lists to None here prevents further
        # requests from being created

        print("TODO: clean shutdown")

class Replies(ReceiverEventHandler):
    def __init__(self, connection)
        self._correlation = {} # map of correlation-id to response queue
        self._ready = False
        self._replies = connection.create_receiver(source_address=address, target_address=address, eventHandler=self)
        # can't handle a request until the replies link is active, as we need the peer assigned address
        # TODO need to delay any processing of task queue until this is done

    def prepare_for_response(self, request, reply_queue):
        assert(self._ready)
        if reply_queue:
            request.message_id = uuid.uuid4().hex
            request.reply_to = self._receiver.source_address
            self._correlation[request.message_id] = reply_queue

    def receiver_active(self, receiver_link):
        self._ready = True

    def receiver_remote_closed(self, receiver, error=None):
        LOG.error("Reply subscription closed by peer: %s" % (error or "no error given"))

    def message_received(self, receiver, message, handle):
        if message.correlation_id in self._correlation:
            self._correlation[message.correlation_id].put(message)

class Server(ReceiverEventHandler):
    def __init__(self, connection, addresses, incoming):
        self._incoming = incoming
        for a in addresses:
            self._receivers.append(connection.create_receiver(source_address=a, target_address=a, eventHandler=self)

    def receiver_remote_closed(self, receiver, error=None):
        LOG.error("Server subscription %s closed by peer: %s" % (receiver.source_address or receiver.target_address, error or "no error given"))

    def message_received(self, receiver, message, handle):
        incoming.put(message)

class ProtocolManager(Object):
    def __init__(self, host, port=5672):
        self._container_io = ContainerIO()
        self._connection = _container_io.connect(host, port)
        self._replies = Replies(self._connection)
        self._senders = {}
        self._servers = {}

    def request(self, target, request, reply_queue=None):
        """ send a request to the given target, and arrange for a
            response to be put on the optional reply_queue if specified"""

        address = self._resolve(target)
        self._replies.prepare_for_response(request, reply_queue)
        self._sender(address).send(request)

    def response(self, address, response):
        self._sender(address).send(response)

    def subscribe(self, target, request_queue):
        assert(target.topic)
        assert(target.server)
        server_address = self._add_server_prefix("%s.%s" % (target.topic, target.server))
        broadcast_address = self._add_broadcast_prefix("broadcast.%s" % target.topic)
        group_address = self._add_group_request_prefix(target.topic)
        self._servers[target] = Server(self._connection, [server_address, group_address, broadcast_address], request_queue)

    def _resolve(self, target):
        if target.server:
            address = self._add_server_prefix("%s.%s" % (target.topic, target.server))
        else if target.fanout:
            address = self._add_broadcast_prefix("broadcast.%s" % target.topic)
        else:
            address = self._add_group_request_prefix(target.topic)
        return address

    def _sender(self, address):
        # if we already have a sender for that address, use it
        # else establish the sender and cache it
        if address in self._senders:
            sender = self._senders[address]
        else:
            sender = self._connection.create_sender(source_address=address, target_address=address)
            self._senders[address] = sender
        return sender

class Task(object):
    @abc.abstractmethod
    def execute(self, manager):
        """Perform operation on the protocol manager (will be called on a thread of its choosing)"""


class SendTask(ProtonTask):
    def __init__(self, target, request, reply_expected):
        self._target = target
        self._request = request
        if reply_expected:
            self._reply_queue = moves.queue.Queue()

    def execute(self, manager):
        manager.request(self._target, self._request, self._reply_queue)

    def get_reply(self, timeout):
        if not self._reply_queue: return None
        return self._reply_queue.get(timeout);

class ListenTask(ProtonTask):
    def __init__(self, target, request_queue):
        self._target = target
        self._request_queue = _request_queue

    def execute(self, manager):
        manager.subscribe(self._target, self._request_queue)

class ReplyTask(ProtonTask):
    def __init__(self, address, response, log_failure):
        self._address = address
        self._response = response
        self._log_failure = log_failure

    def execute(self, manager):
        manager.reply(self._address, self._response)


class ProtonIncomingMessage(base.IncomingMessage):
    def __init__(self, listener, ctxt, message, task_queue):
        base.IncomingMessage.__init__(listener, ctxt, message)
        self._task_queue = task_queue
        self._reply_to = message.reply_to

    def reply(self, reply=None, failure=None, log_failure=True):
        if reply:
            response = marshal_reply(reply)
        else if failure:
            response = marshal_failure(failure)
        else:
            return
        self._task_queue.put(ReplyTask(self._reply_to, response, log_failure))

class ProtonListenerImpl(base.Listener):
    def __init__(self, driver, target):
        base.Listener.__init__(self, driver, target, task_queue)
        self._task_queue = task_queue
        self._incoming = moves.queue.Queue()

    def poll(self):
        request, ctxt = unmarshal_request(self._incoming.get())
        return ProtonIncomingMessage(self, ctxt, request, self._task_queue)

    def incoming():
        return self._incoming

class ProtonDriverImpl(base.BaseDriver):

    def __init__(self, conf, url,
                 default_exchange=None, allowed_remote_exmods=[]):

        base.BaseDriver.__init__(self, conf, url, default_exchange,
                                 allowed_remote_exmods)

    def send(self, target, ctxt, message,
             wait_for_reply=None, timeout=None, envelope=False):
        """Send a message to the given target."""
        request = marshal_request(message, ctxt, envelope, timeout)
        task = SendTask(target, request, wait_for_reply)
        self._tasks.put(task)
        reply = task.get_reply()
        if reply:
            unmarshal_response(reply)

    def send_notification(self, target, ctxt, message, version):
        """Send a notification message to the given target."""

    def listen(self, target):
        """Construct a Listener for the given target."""
        listener = ProtonListenerImpl(self, target, self.tasks)
        self._tasks.put(ListenTask(target, listener._incoming))
        return listener

    def cleanup(self):
        """Release all resources."""
        self._protocol_mgr.destroy()
