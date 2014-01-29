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

from six import moves

from oslo.config import cfg
from oslo.messaging._drivers import base
from oslo.messaging._drivers import common
from oslo.messaging._drivers.protocols.amqp import controller
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

    cfg.StrOpt('amqp_container_name',
               default='',
               help='Name for the AMQP container')

    # @todo: other options?
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
        return common.deserialize_remote_exception(failure, allowed)
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
        conf.register_opts(proton_opts)
        # TODO(grs): handle ssl, authentication etc
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
        self._mgr.connect()

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
            return unmarshal_response(reply, self._allowed_remote_exmods)
        else:
            return None

    def send_notification(self, target, ctxt, message, version):
        """Send a notification message to the given target."""
        return self.send(target, ctxt, message, envelope=(version == 2.0))

    def listen(self, target):
        """Construct a Listener for the given target."""
        LOG.debug("Listen to %s" % target)
        listener = ProtonListener(self, target, self._mgr.tasks())
        self._mgr.tasks().put(ListenTask(target, listener._incoming))
        return listener

    def cleanup(self):
        """Release all resources."""
        LOG.debug("Cleaning up ProtonDriver")
        self._mgr.destroy()
