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
import logging
import threading

from six import moves

from oslo.config import cfg
from oslo import messaging
from oslo.messaging._drivers import base
from oslo.messaging._drivers import common
from oslo.messaging._drivers.protocols.amqp import controller
from oslo.messaging.openstack.common import importutils
from oslo.messaging.openstack.common import jsonutils
from oslo.messaging import target as messaging_target

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

    cfg.StrOpt('amqp_container_name',
               default='',
               help='Name for the AMQP container'),

    cfg.IntOpt('amqp_idle_timeout',
               default=0,  # disabled
               help='Timeout for inactive connections (in seconds)'),

    cfg.BoolOpt('amqp_trace',
                default=False,
                help='Debug: dump AMQP frames to stdout'),

    cfg.StrOpt('proton_ca_file',
               default='',
               help="CA certificate PEM file for verifing server certificate"),

    cfg.StrOpt('proton_cert_file',
               default='',
               help='Identifying certificate to present to clients'),

    cfg.StrOpt('proton_key_file',
               default='',
               help='Private key used to sign cert_file certificate'),

    cfg.StrOpt('proton_key_password',
               default=None,
               help='Password for decrypting key_file (if encrypted)'),

    cfg.BoolOpt('allow_insecure_clients',
                default=False,
                help='Accept clients using either SSL or plain TCP')
]


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
        else:
            self._reply_queue = None

    def execute(self, manager):
        manager.request(self._target, self._request, self._reply_queue)

    def get_reply(self, timeout):
        if not self._reply_queue:
            return None
        try:
            return self._reply_queue.get(timeout=timeout)
        except moves.queue.Empty:
            raise messaging.MessagingTimeout('Timed out waiting for a reply')


class ListenTask(Task):
    def __init__(self, target, incoming, notifications=False):
        self._target = target
        self._incoming = incoming
        self._notifications = notifications

    def execute(self, manager):
        if self._notifications:
            manager.subscribe_notifications(self._target, self._incoming)
        else:
            manager.subscribe(self._target, self._incoming)


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
        failure = common.serialize_remote_exception(failure)
        data = {"failure": failure}
    else:
        data = {"response": reply}
    msg.body = jsonutils.dumps(data)
    return msg


def unmarshal_response(message, allowed):
    data = jsonutils.loads(message.body)
    if "response" in data:
        return data["response"]
    elif "failure" in data:
        #TODO(grs)
        failure = data["failure"]
        raise common.deserialize_remote_exception(failure, allowed)
    else:
        #TODO(grs)
        #???
        return data


def marshal_request(request, context, envelope):
    msg = proton.Message()
    if envelope:
        request = common.serialize_msg(request)
    data = {
        "request": request,
        "context": context
    }
    msg.body = jsonutils.dumps(data)
    return msg


def unmarshal_request(message):
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
        if self._reply_to:
            response = marshal_response(reply=reply, failure=failure)
            if self._correlation_id:
                response.correlation_id = self._correlation_id
            LOG.debug("Replying to %s" % self._correlation_id)
            task = ReplyTask(self._reply_to, response, log_failure)
            self._task_queue.put(task)
        else:
            LOG.debug("Ignoring reply as no reply address available")


class ProtonListener(base.Listener):
    def __init__(self, driver, tasks):
        base.Listener.__init__(self, driver)
        self._tasks = tasks
        self._incoming = moves.queue.Queue()

    def poll(self):
        message = self._incoming.get()
        request, ctxt = unmarshal_request(message)
        LOG.debug("Returning incoming message")
        return ProtonIncomingMessage(self, ctxt, request, self._tasks, message)

    def incoming(self):
        return self._incoming


class ProtonDriver(base.BaseDriver):

    def __init__(self, conf, url,
                 default_exchange=None, allowed_remote_exmods=[]):
        base.BaseDriver.__init__(self, conf, url, default_exchange,
                                 allowed_remote_exmods)
        conf.register_opts(proton_opts)
        # TODO(grs): handle authentication etc
        hosts = [(h.hostname, h.port or 5672) for h in url.hosts]
        cname = conf.amqp_container_name if conf.amqp_container_name else None
        self._mgr = controller.Controller(hosts, cname)
        if conf.server_request_prefix:
            self._mgr.server_request_prefix = conf.server_request_prefix
        if conf.broadcast_prefix:
            self._mgr.broadcast_prefix = conf.broadcast_prefix
        if conf.group_request_prefix:
            self._mgr.group_request_prefix = conf.group_request_prefix
        self._mgr.default_exchange = default_exchange
        self._mgr.idle_timeout = conf.amqp_idle_timeout
        self._mgr.trace_protocol = conf.amqp_trace

        # SSL configuration
        self._mgr.ssl_ca_file = conf.proton_ca_file
        self._mgr.ssl_cert_file = conf.proton_cert_file
        self._mgr.ssl_key_file = conf.proton_key_file
        self._mgr.ssl_key_password = conf.proton_key_password
        self._mgr.ssl_allow_insecure = conf.allow_insecure_clients

        self._connected = False
        self._lock = threading.Lock()

    def send(self, target, ctxt, message,
             wait_for_reply=None, timeout=None, envelope=False):
        """Send a message to the given target."""
        self._ensure_connected()
        request = marshal_request(message, ctxt, envelope)
        if timeout:
            request.ttl = timeout
        task = SendTask(target, request, wait_for_reply)
        self._mgr.tasks().put(task)
        if wait_for_reply:
            LOG.debug("Send to %s, response expected" % target)
            reply = task.get_reply(timeout)
            result = unmarshal_response(reply, self._allowed_remote_exmods)
            LOG.debug("Send to %s returning" % target)
            return result
        else:
            LOG.debug("Send to %s, no response expected" % target)
            return None

    def send_notification(self, target, ctxt, message, version):
        """Send a notification message to the given target."""
        self._ensure_connected()
        return self.send(target, ctxt, message, envelope=(version == 2.0))

    def listen(self, target):
        """Construct a Listener for the given target."""
        LOG.debug("Listen to %s" % target)
        self._ensure_connected()
        listener = ProtonListener(self, self._mgr.tasks())
        self._mgr.tasks().put(ListenTask(target, listener._incoming))
        return listener

    def listen_for_notifications(self, targets_and_priorities):
        LOG.debug("Listen for notifications %s" % targets_and_priorities)
        self._ensure_connected()
        listener = ProtonListener(self, self._mgr.tasks())
        for target, priority in targets_and_priorities:
            topic = '%s.%s' % (target.topic, priority)
            t = messaging_target.Target(topic=topic)
            self._mgr.tasks().put(ListenTask(t, listener._incoming, True))
        return listener

    def _need_connect(self):
        with self._lock:
            if not self._connected:
                self._connected = True
                return True
            else:
                return False

    def _ensure_connected(self):
        # Cause manager to connect when first used. It is safe to push
        # tasks to it whether connected or not, but those tasks won't
        # be processed until connection completes.
        if self._need_connect():
            self._mgr.connect()

    def cleanup(self):
        """Release all resources."""
        LOG.debug("Cleaning up ProtonDriver")
        self._mgr.destroy()
