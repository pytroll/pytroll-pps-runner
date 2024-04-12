#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2018 - 2021 Pytroll Developers

# Author(s):

#   Adam Dybbroe <Firstname.Lastname at smhi.se>

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

"""Publisher and Listener classes for the PPS runners.
"""

import posttroll.subscriber
from posttroll.publisher import Publish
import threading
from nwcsafpps_runner.utils import (SUPPORTED_PPS_SATELLITES,
                                    SUPPORTED_SEVIRI_SATELLITES)

import logging
LOG = logging.getLogger(__name__)


class FileListener(threading.Thread):

    def __init__(self, queue, subscribe_topics):
        threading.Thread.__init__(self)
        self.loop = True
        self.queue = queue
        self.subscribe_topics = subscribe_topics

    def stop(self):
        """Stops the file listener."""
        self.loop = False
        self.queue.put(None)

    def run(self):

        LOG.debug("Subscribe topics = %s", str(self.subscribe_topics))
        with posttroll.subscriber.Subscribe("", self.subscribe_topics, True) as subscr:

            for msg in subscr.recv(timeout=90):
                if not self.loop:
                    break

                # Check if it is a relevant message:
                if self.check_message(msg):
                    LOG.info("Put the message on the queue...")
                    LOG.debug("Message = " + str(msg))
                    self.queue.put(msg)

    def check_message(self, msg):

        if not msg:
            return False

        if ('platform_name' not in msg.data or
                'start_time' not in msg.data):
            LOG.warning("Message is lacking crucial fields...")
            return False
        #: Orbit_number not needed for seviri
        if (msg.data['platform_name'] not in SUPPORTED_SEVIRI_SATELLITES):
            if ('orbit_number' not in msg.data):
                LOG.warning("Message is lacking crucial fields...")
                return False
        if (msg.data['platform_name'] not in SUPPORTED_PPS_SATELLITES):
            LOG.info(str(msg.data['platform_name']) + ": " +
                     "Not a NOAA/Metop/S-NPP/Terra/Aqua scene. Continue...")
            return False

        return True


class FilePublisher(threading.Thread):
    """A publisher for the PPS result files.

    Picks up the return value from the
    pps_worker when ready, and publishes the files via posttroll
    """

    def __init__(self, queue, publish_topic, **kwargs):
        threading.Thread.__init__(self)
        self.loop = True
        self.queue = queue
        self.jobs = {}
        self.publish_topic = publish_topic
        self.runner_name = kwargs.get('runner_name', 'pps_runner')
        self.nameservers = kwargs.get('nameservers')
        if self.nameservers is not None:
            if ',' in self.nameservers:
                self.nameservers = self.nameservers.split(',')
            if not isinstance(self.nameservers, list):
                self.nameservers = [self.nameservers]

    def stop(self):
        """Stops the file publisher."""
        self.loop = False
        self.queue.put(None)

    def run(self):

        with Publish(self.runner_name, 0, self.publish_topic, nameservers=self.nameservers) as publisher:

            while self.loop:
                retv = self.queue.get()

                if retv is not None:
                    LOG.info("Publish the files...")
                    publisher.send(retv)
