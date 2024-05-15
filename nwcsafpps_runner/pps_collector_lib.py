#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2018 - 2021 Pytroll Developers

# Author(s):

#   Nina Hakansson <Firstname.Lastname at smhi.se>

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


import signal
import logging

from posttroll.publisher import create_publisher_from_dict_config
from posttroll.subscriber import Subscribe
from nwcsafpps_runner.config import get_config

from nwcsafpps_runner.message_utils import (publish_l1c,
                                            prepare_pps_collector_message)

LOG = logging.getLogger(__name__)
LOOP = True


def _run_subscribe_publisher(subscriber, publisher, options):
    """The posttroll subscribe/publisher runner."""
    def signal_handler(sig, frame):
        LOG.warning('You pressed Ctrl+C!')
        global LOOP
        LOOP = False

    signal.signal(signal.SIGINT, signal_handler)

    while LOOP:
        for msg in subscriber.recv():
            LOG.debug(
                "Received message data = %s", msg)
            pub_msg = prepare_pps_collector_message(msg, options)
            publish_l1c(publisher, pub_msg, publish_topic=[options["publish_topic"]], msg_type="dataset")
            LOG.info("L1c and PPS products collected.")

def pps_collector_runner(config_file):
    """The live runner for collecting the NWCSAF/PPS l1c and lvl2 products."""
    LOG.info("Start the NWCSAF/PPS products and level-1c collector runner")
    
    options = get_config(config_file)
    settings = {"name": 'pps-collector-runner',
                "nameservers": False,
                "port": options.get("publish_port", 0)}
    with Subscribe('', options["subscribe_topics"], True) as sub:
        pub = create_publisher_from_dict_config(settings):
        _run_subscribe_publisher(sub, pub, options)
