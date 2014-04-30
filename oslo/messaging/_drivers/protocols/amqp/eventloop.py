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

import errno
import heapq
import logging
import os
import select
import socket
import sys
import threading
import time
import uuid

from six import moves

from oslo.messaging._drivers.protocols.amqp import engine

LOG = logging.getLogger(__name__)


class _SocketConnection():
    """Associates a proton Connection with a python network socket,
    and handles all connection-related events.
    """

    def __init__(self, name, container, properties, handler):
        self.name = name
        self._properties = properties
        self._handler = handler
        self._container = container
        c = container.create_connection(name, handler, properties)
        c.pn_connection.properties = self._get_name_and_pid()
        c.user_context = self
        self.connection = c

    def _get_name_and_pid(self):
        return {u'process': os.path.basename(sys.argv[0]), u'pid': os.getpid()}

    def fileno(self):
        """Allows use of a _SocketConnection in a select() call.
        """
        return self.socket.fileno()

    def read(self):
        """Called when socket is read-ready."""
        while True:
            try:
                rc = engine.do_input(self.connection, self.socket)
                if rc > 0:
                    self.connection.process(time.time())
                return rc
            except socket.error as e:
                err = e.args[0]
                if err == errno.EAGAIN or err == errno.EINTR:
                    continue
                elif err == errno.EWOULDBLOCK:
                    return 0
                else:
                    self._handler.connection_failed(self.connection, e)
                    return engine.Connection.EOS

    def write(self):
        """Called when socket is write-ready."""
        while True:
            try:
                rc = engine.do_output(self.connection, self.socket)
                if rc > 0:
                    self.connection.process(time.time())
                return rc
            except socket.error as e:
                err = e.args[0]
                if err == errno.EAGAIN or err == errno.EINTR:
                    continue
                elif err == errno.EWOULDBLOCK:
                    return 0
                else:
                    self._handler.connection_failed(self.connection, e)
                    return engine.Connection.EOS

    def connect(self, hostname, port, sasl_mechanisms="ANONYMOUS"):
        addr = socket.getaddrinfo(hostname, port,
                                  socket.AF_INET, socket.SOCK_STREAM)
        if not addr:
            key = "%s:%i" % (hostname, port)
            error = _("Could not translate address '%s'") % key
            LOG.warn(error)
            self.handler.connection_failed(self.connection, error)
        my_socket = socket.socket(addr[0][0], addr[0][1], addr[0][2])
        my_socket.setblocking(0)  # 0=non-blocking
        my_socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        try:
            my_socket.connect(addr[0][4])
        except socket.error as e:
            if e[0] != errno.EINPROGRESS:
                error = _("Socket connect failure '%s'") % str(e)
                LOG.warn(error)
                self.handler.connection_failed(self.connection, str(e))
        self.socket = my_socket

        if sasl_mechanisms:
            pn_sasl = self.connection.pn_sasl
            pn_sasl.mechanisms(sasl_mechanisms)
            # TODO(kgiusti): server if accepting inbound connections
            pn_sasl.client()
        self.connection.open()

    def reset(self, name=None):
        self._container.remove_connection(self.name)
        if name:
            self.name = name
        c = self._container.create_connection(self.name, self._handler,
                                              self._properties)
        c.user_context = self
        self.connection = c

    def close(self):
        self.socket.close()


class Schedule(object):
    def __init__(self):
        self._entries = []

    def schedule(self, request, delay):
        entry = (time.time() + delay, request)
        heapq.heappush(self._entries, entry)

    def timeout(self, t):
        due = self.next()
        if not due:
            return t
        now = time.time()
        if due < now:
            return 0
        else:
            return min(due - now, t) if t else due - now

    def next(self):
        return self._entries[0][0] if len(self._entries) else None

    def process(self):
        n = self.next()
        while n and n < time.time():
            heapq.heappop(self._entries)[1]()
            n = self.next()


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


class Thread(threading.Thread):

    def __init__(self, container_name=None):
        threading.Thread.__init__(self)

        # handle requests from other threads
        self._requests = Requests()
        # handle delayed tasks (only used on this thread for now)
        self._schedule = Schedule()

        # Configure a container
        if container_name:
            self._container = engine.Container(container_name)
        else:
            self._container = engine.Container(uuid.uuid4().hex)

        self.name = "Thread for Proton container: %s" % self._container.name
        self._shutdown = False
        self.daemon = True
        self.start()

    def wakeup(self, request=None):
        """Wake up the processing thread."""
        self._requests.wakeup(request)

    def schedule(self, request, delay):
        self._schedule.schedule(request, delay)

    def destroy(self):
        """Stop the processing thread, releasing all resources.
        """
        LOG.debug("Stopping Proton container %s" % self._container.name)
        self.wakeup(lambda: self.shutdown())
        self.join()

    def shutdown(self):
        self._shutdown = True

    def connect(self, hostname, port, handler, properties={}, name=None,
                sasl_mechanisms="ANONYMOUS"):
        """Get a _SocketConnection to a peer represented by url."""
        key = name or "%s:%i" % (hostname, port)
        # return pre-existing
        conn = self._container.get_connection(key)
        if conn:
            return conn.user_context

        # create a new connection - this will be stored in the
        # container, using the specified name as the lookup key, or if
        # no name was provided, the host:port combination
        sc = _SocketConnection(key, self._container,
                               properties, handler=handler)
        sc.connect(hostname, port, sasl_mechanisms)
        return sc

    def run(self):
        """Run the proton event/timer loop."""
        LOG.debug("Starting Proton thread, container=%s",
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
                deadline = timers[0].deadline  # 0 == next expiring timer
                now = time.time()
                timeout = 0 if deadline <= now else deadline - now

            timeout = self._schedule.timeout(timeout)

            results = select.select(readfds, writefds, [], timeout)
            readable, writable, ignore = results

            for r in readable:
                r.read()

            self._schedule.process()
            for t in timers:
                if t.deadline > time.time():
                    break
                t.process(time.time())

            for w in writable:
                w.write()

        LOG.debug("Stopping Proton thread, container=%s",
                  self._container.name)
        # abort any requests. Setting the lists to None here prevents further
        # requests from being created
