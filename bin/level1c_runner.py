#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2019 - 2021 Pytroll

# Author(s):

#   Erik Johansson <Firstname.Lastname at smhi.se>
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


import argparse
import logging
import sys
import signal

from posttroll.subscriber import Subscribe
from posttroll.publisher import Publish
from nwcsafpps_runner.logger import setup_logging
from nwcsafpps_runner.message_utils import publish_l1c, prepare_l1c_message
from nwcsafpps_runner.l1c_processing import L1cProcessor
from nwcsafpps_runner.l1c_processing import ServiceNameNotSupported
from nwcsafpps_runner.l1c_processing import MessageTypeNotSupported

LOOP = True

LOG = logging.getLogger('l1c-runner')


def _run_subscribe_publisher(l1c_proc, service_name, subscriber, publisher):
    """The porsttroll subscribe/publisher runner."""

    def signal_handler(sig, frame):
        LOG.warning('You pressed Ctrl+C!')
        global LOOP
        LOOP = False

    signal.signal(signal.SIGINT, signal_handler)

    while LOOP:
        for msg in subscriber.recv():
            l1c_proc.initialize(service_name)
            LOG.debug(
                "Received message data = %s", l1c_proc.message_data)

            if not msg:
                continue
            try:
                l1c_proc.run(msg)
            except MessageTypeNotSupported as err:
                LOG.warning(err)
                continue
            if l1c_proc.l1cfile is not None:
                pub_msg = prepare_l1c_message(l1c_proc.l1cfile,
                                              l1c_proc.message_data,
                                              orbit=l1c_proc.orbit_number)
                publish_l1c(publisher, pub_msg,
                            publish_topic=l1c_proc.publish_topic)
                LOG.info("L1C processing has completed.")
            else:
                LOG.warning("L1C processing has failed.")


def l1c_runner(config_filename, service_name):
    """The live runner for the NWCSAF/PPS l1c product generation."""
    LOG.info("Start the NWCSAF/PPS level-1c runner - Service = %s", service_name)

    l1c_proc = L1cProcessor(config_filename, service_name)
    publish_name = service_name + '-runner'
    with Subscribe('', l1c_proc.subscribe_topics, True) as sub:
        with Publish(publish_name, 0, nameservers=l1c_proc.nameservers) as pub:
            _run_subscribe_publisher(l1c_proc, service_name, sub, pub)


def get_arguments():
    """
    Get command line arguments.
    Return
    name of the service and the config filepath
    """

    parser = argparse.ArgumentParser()

    parser.add_argument("-l", "--log-config",
                        help="Log config file to use instead of the standard logging.")
    parser.add_argument('-c', '--config_file',
                        type=str,
                        dest='config_file',
                        default='l1c_config.yaml',
                        help="The file containing " +
                        "configuration parameters e.g. product_filter_config.yaml, \n" +
                        "default = ./l1c_config.yaml",
                        required=True)
    parser.add_argument("-s", "--service",
                        type=str,
                        dest="service",
                        default='seviri-l1c',
                        help="Name of the service (e.g. seviri-l1c), \n" +
                        "default = seviri-l1c")
    parser.add_argument("-v", "--verbose", dest="verbosity", action="count", default=0,
                        help="Verbosity (between 1 and 2 occurrences with more leading to more "
                        "verbose logging). WARN=0, INFO=1, "
                        "DEBUG=2. This is overridden by the log config file if specified.")

    args = parser.parse_args()
    setup_logging(args)

    service = args.service.lower()

    if 'template' in args.config_file:
        raise IOError("Template file given as master config, aborting!")

    return args.config_file, service


if __name__ == '__main__':

    CONFIG_FILENAME, SERVICE_NAME = get_arguments()

    l1c_runner(CONFIG_FILENAME, SERVICE_NAME)
