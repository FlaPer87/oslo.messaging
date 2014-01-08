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

    cfg.BoolOpt('proton_trace',
                default=False,
                help='Enable protocol tracing')
]

JSON_CONTENT_TYPE = 'application/json; charset=utf8'



class _ProtonDaemon(threading.Thread):
    """Manages a thread running an instance of the QPID Proton protocol engine.
    """

    class Empty(Exception):
        """Exception raised by self.fetch() when there is no message available
        within the alloted time.
        """
        pass

    class Timeout(Exception):
        """Exception raised by self.send() when the send does not complete
        within the alloted time.
        """
        pass

    class SendRequest(object):
        """A request to send a single message.
        """
        def __init__(self, msg):
            self.msg = msg
            self._done = threading.Event()
            self.aborted = False

        def wait(self, timeout=None):
            """Wait for send to complete.  Return True if send completed, else
            False if timeout.
            """
            return self._done.wait(timeout)

        def sent(self):
            self._done.set()

        def abort(self):
            """Signal that the send request failed due to thread
            shutdown.
            """
            self.aborted = True
            self._done.set()

    class SubscribeRequest(object):
        """A request to subscribe to a given source of messages.
        """
        def __init__(self, source_address):
            self.source_address = source_address
            self._done = threading.Event()
            self.handle = None

        def wait(self, timeout=None):
            """Wait for subscription request to complete. Returns True when
            subscription completed, else False if timed-out.
            """
            return self._done.wait(timeout)

        def subscribed(self, subscription_id):
            """Signal that the subscription request completed.  The
            subscription_id identifies the new subscription, which can be used
            to cancel it in the future.
            """
            self.handle = subscription_id
            self._done.set()

        def aborted(self):
            """Signal that the subscription request failed due to thread
            shutdown.
            """
            self.handle = None
            self._done.set()

    def __init__(self, conf):
        """Create a _ProtonDaemon.
        """
        threading.Thread.__init__(self)
        self._conf = conf

        self._wakeup_pipe = os.Pipe() # (r,w)

        # pending outgoing messages (SendRequest)
        self._send_requests = moves.queue.Queue()

        # pending subscription requests (SubscribeRequest)
        self._subscribe_requests = moves.queue.Queue()

        # queue of received messages ??? blah
        self._received_msgs = moves.queue.Queue()

        #
        # Configure a container
        #
        container_name = self._conf.amqp10_container_name
        if container_name:
            self._container = proton_wrapper.Container(container_name)
        else:
            self._container = proton_wrapper.Container(uuid.uuid4().hex)

        self.daemon = True
        self.name = "Thread for Proton container: %s" % self._container.name
        self._done = False
        LOG.debug(_("Starting Proton container %s") % self._container.name))
        self.start()

    def wakeup(self):
        """Force the select() to return"""
        os.write(self._wakeup_pipe[1], "!")

    def destroy(self):
        """Stop the Proton thread, releasing all resources.
        """
        LOG.debug("Stopping Proton container %s" % self._container.name)
        self._done = True
        self.wakeup()
        self.join()

    def send(self, msg, timeout=None):
        """Enqueue msg for eventual transmit.
        """
        LOG.debug("sending msg %s" % str(msg))
        req = MessengerDaemon._SendRequest(msg)
        self._client_requests_lock.acquire()
        try:
            if self._subscribe_requests is None:
                # Messenger thread has shutdown
                raise MessengerDaemon.Timeout()
            self._send_requests.append(req)
        finally:
            self._client_requests_lock.release()
        self._messenger.interrupt()
        # @todo Need some way of aborting wait if Messenger thread exits!
        if not req.wait(timeout):
            raise MessengerDaemon.Timeout()
        LOG.debug("sent msg %s" % str(msg))

    def subscribe(self, source_address):
        """Subscribe to source address. Causes messages to arrive from the
        source_address. Returns handle which is used to unsubscribe.
        """
        req = MessengerDaemon._SubscribeRequest(source_address)
        self._client_requests_lock.acquire()
        try:
            if self._subscribe_requests is None:
                # Messenger thread has shutdown
                return None
            self._subscribe_requests.append(req)
        finally:
            self._client_requests_lock.release()
        self._messenger.interrupt()
        # @TODO(kgiusti) Need some way of aborting wait if Messenger thread
        # exits!
        req.wait()
        LOG.debug("subscribed to %s" % str(source_address))
        return req.handle

    def unsubscribe(self, handle):
        """Cancel a subscription.  Any pending messages will be discarded.
        Handle was returned by the subscribe() method.
        """
        # @TODO(kgiusti): currently Messenger cannot unsubscribe, see
        # https://issues.apache.org/jira/browse/PROTON-142
        LOG.debug("unsubscribed %s (%s)" % handle)

    def fetch(self, timeout=None):
        """Retrieve the next available received message, block until a message
        is available or timeout expires. Throws Empty if no message retrieved.
        """
        try:
            return self._received_msgs.get(True, timeout)
        except Queue.Empty:
            raise MessengerDaemon.Empty()

    def run(self):
        """Thread body."""
        available_credit = 0
        self._messenger.start()
        while not self._done:

            subs = []
            sends = []

            # Gather all pending requests
            self._client_requests_lock.acquire()
            try:
                subs = self._subscribe_requests
                self._subscribe_requests = []
                sends = self._send_requests
                self._send_requests = []
            finally:
                self._client_requests_lock.release()

            # Create all requested subscriptions
            for req in subs:
                addr = self._url_prefix + req.source_address
                self._messenger.subscribe(addr)
                LOG.debug(_("subscribed to '%s'") % addr)
                req.subscribed(addr)
                available_credit += 10

            # Send all outbound messages
            sent = 0
            for req in sends:
                self._messenger.put(req.msg)
                # @todo - do not complete the request until the message has
                # reached a terminal state (ie, monitor the tracker).
                # For now, the send is unreliable
                #self._messenger.send()
                req.sent()
                sent += 1

            # Process all received messages
            while self._messenger.incoming > 0:
                msg = qpid_proton.Message()
                self._messenger.get(msg)
                #available_credit += 1
                self._received_msgs.put(msg)

            # do protocol processing
            try:
                # @TODO(kgiusti): can't grant credit without recv(), can't call
                # recv() without setting up subscriptions.  Awkward.
                if available_credit > 0:
                    #c = available_credit
                    #available_credit = 0
                    #print("M: granting credit = %d" % c)
                    #self._messenger.recv(c)
                    self._messenger.recv(available_credit)
                else:
                    self._messenger.work()
            except qpid_proton.Interrupt:
                pass

        LOG.debug(_("Messenger thread stopping..."))

        # abort any requests. Setting the lists to None here prevents further
        # requests from being created

        self._client_requests_lock.acquire()
        try:
            subs = self._subscribe_requests
            self._subscribe_requests = None
            sends = self._send_requests
            self._send_requests = None
        finally:
            self._client_requests_lock.release()

        for req in subs + sends:
            req.abort()
            print("REQ ABORT")

        # flush any pending sends...
        try:
            while self._messenger.outgoing > 0:
                # @TODO(kgiusti) timeout?
                self._messenger.send()
        except Exception:
            LOG.warning(_("Messenger failed to flush outgoing messages"))

        self._messenger.stop()
        LOG.debug(_("Messenger stopped"))



class ProtonDriver(base.BaseDriver):

    def __init__(self, conf, url,
                 default_exchange=None, allowed_remote_exmods=[]):
        if not proton:
            raise ImportError("Failed to import Qpid Proton library")

        super(ProtonDriver, self).__init__(conf, url,
                                           default_exchange,
                                           allowed_remote_exmods)
        conf.register_opts(proton_opts)
        self._daemon = _ProtonDaemon(conf)

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


