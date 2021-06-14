#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2021 Adam.Dybbroe

# Author(s):

#   Adam.Dybbroe <a000680@c21856.ad.smhi.se>

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

"""Message utilities.
"""

import os
import logging
import socket
from urllib.parse import urlunsplit
from posttroll.message import Message

LOG = logging.getLogger(__name__)


def prepare_l1c_message(result_file, mda, **kwargs):
    """Prepare the output message for the level-1c file creation."""

    if not result_file:
        return

    # Now publish:
    to_send = mda.copy()
    # Delete the input level-1 dataset from the message:
    try:
        del to_send['dataset']
    except KeyError:
        LOG.warning("Couldn't remove dataset from message")

    if ('orbit' in kwargs) and ('orbit_number' in to_send.keys()):
        to_send["orig_orbit_number"] = to_send["orbit_number"]
        to_send["orbit_number"] = kwargs['orbit']

    to_send["uri"] = urlunsplit(('ssh', socket.gethostname(), result_file, '', ''))
    filename = os.path.basename(result_file)
    to_send["uid"] = filename

    to_send['format'] = 'PPS-L1C'
    to_send['type'] = 'NETCDF'
    to_send['data_processing_level'] = '1c'

    return to_send


def publish_l1c(publisher, publish_msg, publish_topic):
    """Publish the messages that l1c files are ready."""

    LOG.debug('Publish topic = %s', publish_topic)
    for topic in publish_topic:
        msg = Message(topic, "file", publish_msg).encode()
        LOG.debug("sending: %s", str(msg))
        publisher.send(msg)
