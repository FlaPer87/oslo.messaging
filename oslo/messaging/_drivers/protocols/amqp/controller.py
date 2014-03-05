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

import logging
import uuid

from six import moves

from oslo.messaging._drivers.protocols.amqp import engine
from oslo.messaging._drivers.protocols.amqp import eventloop

LOG = logging.getLogger(__name__)


class Replies(engine.ReceiverEventHandler):
    def __init__(self, connection, on_ready):
        self._correlation = {}  # map of correlation-id to response queue
        self._ready = False
        self._on_ready = on_ready
        self._receiver = connection.create_receiver("replies",
                                                    event_handler=self)
        self.capacity = 100  # somewhat arbitrary
        self._credit = 0

    def ready(self):
        return self._ready

    def prepare_for_response(self, request, reply_queue):
        if reply_queue:
            request.id = uuid.uuid4().hex
            self._correlation[request.id] = reply_queue
            request.reply_to = self._receiver.source_address
            LOG.debug("Reply for %s expected on %s" %
                      (request.id, request.reply_to))

    def receiver_active(self, receiver_link):
        self._ready = True
        self._update_credit()
        self._on_ready()
        LOG.debug("Replies expected on %s" % self._receiver.source_address)

    def receiver_remote_closed(self, receiver, error=None):
        # TODO(grs)
        LOG.error("Reply subscription closed by peer: %s",
                  (error or "no error given"))

    def message_received(self, receiver, message, handle):
        self._credit = self._credit - 1
        self._update_credit()

        key = message.correlation_id
        if key in self._correlation:
            LOG.debug("Received response for %s" % key)
            self._correlation[key].put(message)
            # cleanup (only need one response per request)
            del self._correlation[key]
        else:
            LOG.warn("Can't correlate %s (%s)" % (key, self._correlation))
        receiver.message_accepted(handle)

    def _update_credit(self):
        if self.capacity > self._credit:
            self._receiver.add_capacity(self.capacity - self._credit)
            self._credit = self.capacity


class Server(engine.ReceiverEventHandler):
    def __init__(self, connection, addresses, incoming):
        self._incoming = incoming
        self._addresses = addresses
        self.attach(connection)

    def attach(self, connection):
        self._receivers = []
        for a in self._addresses:
            r = connection.create_receiver(source_address=a,
                                           target_address=a,
                                           event_handler=self)
            r.add_capacity(1)  # TODO(grs)
            self._receivers.append(r)

    def receiver_remote_closed(self, receiver, error=None):
        text = "Server subscription %(addr)s closed by peer: %(err_msg)s"
        vals = {
            "addr": receiver.source_address or receiver.target_address,
            "err_msg": error or "no error given"
        }
        LOG.error(text % vals)

    def message_received(self, receiver, message, handle):
        receiver.add_capacity(1)
        self._incoming.put(message)
        LOG.debug("message received: %s" % message)
        receiver.message_accepted(handle)


class Hosts(object):
    def __init__(self, entries=[]):
        self._entries = entries
        self._current = 0

    def add(self, hostname, port=5672):
        self._entries.append((hostname, port))

    def current(self):
        if len(self._entries):
            return self._entries[self._current]
        else:
            return ("localhost", 5672)

    def next(self):
        if len(self._entries) > 1:
            self._current = (self._current + 1) % len(self._entries)
        return self.current()

    def hostname(self):
        return self.current()[0]

    def port(self):
        return self.current()[1]

    def __repr__(self):
        return '<Hosts ' + str(self) + '>'

    def __str__(self):
        return ", ".join(["%s:%i" % e for e in self._entries])


class Controller(engine.ConnectionEventHandler):
    def __init__(self, hosts=None, container_name=None):
        self.processor = None
        self._container_name = container_name
        self._tasks = moves.queue.Queue()
        self._senders = {}
        self._servers = {}
        self.hosts = Hosts(hosts)
        self.separator = "."
        self.fanout_qualifier = "all"
        self.server_request_prefix = "exclusive"
        self.broadcast_prefix = "broadcast"
        self.group_request_prefix = "unicast"
        self.default_exchange = None
        self.idle_timeout = 0
        self.trace_protocol = False
        self.ssl_ca_file = None
        self.ssl_cert_file = None
        self.ssl_key_file = None
        self.ssl_key_password = None
        self.ssl_allow_insecure = False
        self._delay = 0
        # can't handle a request until the replies link is active, as
        # we need the peer assigned address, so need to delay any
        # processing of task queue until this is done
        self._replies = None

    def connect(self):
        self.processor = eventloop.Thread(self._container_name)
        self.processor.wakeup(lambda: self._do_connect())

    def _do_connect(self):
        """Establish connection and reply subscripion on processor thread."""
        hostname, port = self.hosts.current()
        conn_props = {}
        if self.idle_timeout:
            conn_props["idle-time-out"] = float(self.idle_timeout)
        if self.trace_protocol:
            conn_props["x-trace-protocol"] = self.trace_protocol
        if self.ssl_ca_file:
            conn_props["x-ssl-ca-file"] = self.ssl_ca_file
        if self.ssl_cert_file:
            # assume this connection is for a server.  If client authentication
            # support is developed, we'll need an explict flag (server or
            # client)
            conn_props["x-ssl-server"] = True
            conn_props["x-ssl-identity"] = (self.ssl_cert_file,
                                            self.ssl_key_file,
                                            self.ssl_key_password)
            conn_props["x-ssl-allow-cleartext"] = self.ssl_allow_insecure
        self._socket_connection = self.processor.connect(hostname, port,
                                                         handler=self,
                                                         properties=conn_props)
        self._connection = self._socket_connection.connection
        LOG.debug("Connection initiated")

    def _do_reconnect(self):
        self._senders = {}
        self._socket_connection.reset()
        self._connection = self._socket_connection.connection
        hostname, port = self.hosts.next()
        self._socket_connection.connect(hostname, port)
        LOG.info("Reconnecting to: %s:%i" % (hostname, port))

    def connection_failed(self, connection, error):
        LOG.debug("Connection failure: %s" % error)
        if self._connection == connection or self._replies:
            self._replies = None
            if self._delay == 0:
                self._delay = 1
                self._do_reconnect()
            else:
                d = self._delay
                LOG.info("delaying reconnect attempt for %d seconds" % d)
                self.processor.schedule(lambda: self._do_reconnect(), d)
                self._delay = min(d * 2, 60)

    def connection_active(self, connection):
        h = self.hosts.current()
        LOG.debug("Connection active (%s:%i), subscribing..." % h)
        for s in self._servers.itervalues():
            s.attach(self._connection)
        self._replies = Replies(self._connection, lambda: self._ready())
        self._delay = 0

    def connection_closed(self, connection, reason):
        if reason:
            LOG.info("Connection closed: %s" % reason)
        else:
            LOG.info("Connection closed")

    def _ready(self):
        LOG.info("Messaging is active (%s:%i)" % self.hosts.current())
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
                LOG.exception("Error processing task: %s", e)

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
        LOG.debug("Sending request for %s to %s" % (target, address))
        self._replies.prepare_for_response(request, reply_queue)
        self._send(address, request)

    def response(self, address, response):
        LOG.debug("Sending response to %s" % address)
        self._send(address, response)

    def subscribe(self, target, requests):
        addresses = [
            self._server_address(target),
            self._broadcast_address(target),
            self._group_request_address(target)
        ]
        self._subscribe(target, addresses, requests)

    def subscribe_notifications(self, target, requests):
        addresses = [self._group_request_address(target)]
        self._subscribe(target, addresses, requests)

    def _subscribe(self, target, addresses, requests):
        LOG.debug("Subscribing to %s (%s)" % (target, addresses))
        self._servers[target] = Server(self._connection, addresses, requests)

    def _resolve(self, target):
        if target.server:
            return self._server_address(target)
        elif target.fanout:
            return self._broadcast_address(target)
        else:
            return self._group_request_address(target)

    def _send(self, addr, message):
        address = str(addr)
        message.address = address
        self._sender(address).send(message)

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

    def _server_address(self, target):
        return self._concatenate([self.server_request_prefix,
                                  target.exchange or self.default_exchange,
                                  target.topic, target.server])

    def _broadcast_address(self, target):
        return self._concatenate([self.broadcast_prefix,
                                  target.exchange or self.default_exchange,
                                  target.topic, self.fanout_qualifier])

    def _group_request_address(self, target):
        return self._concatenate([self.group_request_prefix,
                                  target.exchange or self.default_exchange,
                                  target.topic])

    def _concatenate(self, items):
        return self.separator.join(filter(bool, items))
