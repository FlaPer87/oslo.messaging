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



class ProtonDriver(base.BaseDriver, threading.Thread):

    def __init__(self, conf, url,
                 default_exchange=None, allowed_remote_exmods=[]):
        if not proton:
            raise ImportError("Failed to import Qpid Proton library")

        threading.Thread.__init__(self)
        base.BaseDriver.__init__(self, conf, url, default_exchange,
                                 allowed_remote_exmods)
        self.conf.register_opts(proton_opts)

        # pending outgoing messages (SendRequest)
        self._send_requests = moves.queue.Queue()

        # pending subscription requests (SubscribeRequest)
        self._subscribe_requests = moves.queue.Queue()

        # queue of received messages ??? blah
        self._received_msgs = moves.queue.Queue()

        #
        # Configure a container
        #
        container_name = self.conf.amqp10_container_name
        if container_name:
            self._container = proton_wrapper.Container(container_name)
        else:
            self._container = proton_wrapper.Container(uuid.uuid4().hex)



        # run the Proton Engine event/timer thread:

        self._wakeup_pipe = os.Pipe() # (r,w) - to force return from select()
        self._proton_lock = threading.Lock()

        self.name = "Thread for Proton container: %s" % self._container.name
        self._shutdown = False
        self.daemon = False # KAG: or True ???
        self.start()

    def wakeup(self):
        """Force the Proton thread to wake up"""
        os.write(self._wakeup_pipe[1], "!")

    def destroy(self):
        """Stop the Proton thread, releasing all resources.
        """
        LOG.debug("Stopping Proton container %s" % self._container.name)
        self._shutdown = True
        self.wakeup()
        self.join()

    def get_connection(self, url, properties={}):
        """Get a _SocketConnection to a peer represented by url"""
        # @todo KAG: for now, assume amqp://<hostname>[:port]
        regex = re.compile(r"^amqp://([a-zA-Z0-9.]+)(:([\d]+))?$")
        x = regex.match(url)
        if not x:
            error = "Bad URL syntax: %s" % url
            LOG.warn(_(error))
            raise Exception(error)

        # return pre-existing
        with self._proton_lock:
            conn = self._container.get_connection(url)
        if conn:
            assert isinstance(conn.user_context, _SocketConnection)
            return conn.user_context

        # create new connection
        matches = x.groups()
        host = matches[0]
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

        # create a new connection - this will be stored in the container,
        # using the url as the lookup key
        sc = _SocketConnection(url, my_socket,
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

            readfd = [self._wakeup_pipe[0]]
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

            while os.read(self._wakeup_pipe[0], 512):
                pass

            for r in readable:
                if r is self._wakeup_pipe[0]: continue
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

    #
    # BaseDriver Interface:
    #
    def send(self, target, ctxt, message,
             wait_for_reply=None, timeout=None, envelope=False):
        """Send a message to the given target."""
        assert False, "NOT YET IMPLEMENTED"

    def send_notification(self, target, ctxt, message, version):
        """Send a notification message to the given target."""
        assert False, "NOT YET IMPLEMENTED"

    def listen(self, target):
        """Construct a Listener for the given target."""
        assert False, "NOT YET IMPLEMENTED"

    def cleanup(self):
        """Release all resources."""
        assert False, "NOT YET IMPLEMENTED"
        self._daemon.destroy()


