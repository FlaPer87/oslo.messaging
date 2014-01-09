#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
import socket, errno, logging

from connection import Connection

LOG = logging.getLogger(__name__)

"""helper methods that provide boilerplate socket I/O and Connection
    processing.
"""

def read_socket_input(connection, socket):
    """Read from the network layer and processes all data read.  Can
    support both blocking and non-blocking sockets.
    Returns the number of input bytes processed, or EOS if input processing
    is done.  Any exceptions raised by the socket are re-raised.
    """
    count = connection.needs_input
    if count <= 0:
        return count  # 0 or EOS

    try:
        sock_data = socket.recv(count)
    except socket.timeout, e:
        LOG.debug("Socket timeout exception %s", str(e))
        raise  # caller must handle
    except socket.error, e:
        LOG.debug("Socket error exception %s", str(e))
        err = e.args[0]
        # ignore non-fatal errors
        if (err != errno.EAGAIN and
            err != errno.EWOULDBLOCK and
            err != errno.EINTR):
            # otherwise, unrecoverable:
            connection.close_input()
        raise  # caller must handle
    except Exception, e:  # beats me... assume fatal
        LOG.debug("unknown socket exception %s", str(e))
        connection.close_input()
        raise  # caller must handle

    if sock_data:
        count = connection.process_input( sock_data )
    else:
        LOG.debug("Socket closed")
        count = Connection.EOS
        connection.close_input()
    return count

def write_socket_output(connection, socket):
    """Write data to the network layer.  Can support both blocking and
    non-blocking sockets.
    """
    count = connection.has_output
    if count <= 0:
        return count # 0 or EOS

    data = connection.output_data()
    try:
        count = socket.send(data)
    except socket.timeout, e:
        LOG.debug("Socket timeout exception %s", str(e))
        raise # caller must handle
    except socket.error, e:
        LOG.debug("Socket error exception %s", str(e))
        err = e.args[0]
        # ignore non-fatal errors
        if (err != errno.EAGAIN and
            err != errno.EWOULDBLOCK and
            err != errno.EINTR):
            # otherwise, unrecoverable
            connection.close_output()
        raise
    except Exception, e:  # beats me... assume fatal
        LOG.debug("unknown socket exception %s", str(e))
        connection.close_output()
        raise

    if count > 0:
        connection.output_written(count)
    elif data:
        LOG.debug("Socket closed")
        count = Connection.EOS
        connection.close_output()
    return count


__all__ = [
    "read_socket_input",
    "write_socket_output"
    ]