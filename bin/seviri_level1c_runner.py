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
import socket
import logging
import sys
import os
from multiprocessing import cpu_count
from urllib.parse import urlunsplit

from level1c4pps.seviri2pps_lib import process_one_scan  # @UnresolvedImport
from posttroll.subscriber import Subscribe  # @UnresolvedImport
from posttroll.publisher import Publish  # @UnresolvedImport
from posttroll.message import Message  # @UnresolvedImport
from nwcsafpps_runner.config import get_config_from_yamlfile as get_config
from nwcsafpps_runner.utils import (deliver_output_file, cleanup_workdir)
from nwcsafpps_runner.logger import setup_logging


SUPPORTED_METEOSAT_SATELLITES = ['meteosat-8', 'meteosat-9', 'meteosat-10', 'meteosat-11']

#LOG = logging.getLogger(__name__)
LOG = logging.getLogger('seviri-l1c-runner')


class L1cProcessor(object):
    """Container for the SEVIRI HRIT processing."""

    def __init__(self, ncpus):
        from multiprocessing.pool import ThreadPool
        self.pool = ThreadPool(ncpus)
        self.ncpus = ncpus

        self.sensor = "unknown"
        self.orbit_number = 99999  # Initialised orbit number
        self.platform_name = 'unknown'
        self.l1c_result = None
        self.pass_start_time = None
        self.l1cfile = None
        self.level1_files = []

        self.result_home = OPTIONS.get('output_dir', '/tmp')
        self.working_home = OPTIONS.get('working_dir', '/tmp')
        self.publish_topic = OPTIONS.get('publish_topic')
        self.site = OPTIONS.get('site', 'unknown')
        self.environment = OPTIONS.get('environment')
        self.message_data = None
        self.service = None

    def initialise(self, service):
        """Initialise the processor."""
        self.l1c_result = None
        self.pass_start_time = None
        self.l1cfile = None
        self.level1_files = []
        self.service = service

    def deliver_output_file(self, subd=None):
        """Deliver the generated level-1c file to the configured destination."""
        LOG.debug("Result file: %s", str(self.l1cfile))
        LOG.debug("Result home dir: %s", str(self.result_home))
        LOG.debug("Sub directory: %s", subd)
        return deliver_output_file(self.l1cfile, self.result_home, subd)

    def run(self, msg):
        """Start the L1c processing using process_one_scan."""

        if not msg:
            return False

        if msg.type != 'dataset':
            LOG.info("Not a dataset, don't do anything...")
            return False

        if ('platform_name' not in msg.data or
                'start_time' not in msg.data):
            LOG.warning("Message is lacking crucial fields...")
            return False

        if msg.data['platform_name'].lower() not in SUPPORTED_METEOSAT_SATELLITES:
            LOG.info(str(msg.data['platform_name']) + ": " +
                     "Not a valid Meteosat scene. Continue...")
            return False

        self.platform_name = str(msg.data['platform_name'])
        self.sensor = str(msg.data['sensor'])
        self.message_data = msg.data

        level1_dataset = msg.data['dataset']

        if len(level1_dataset) < 1:
            return False

        pro_files = False
        epi_files = False
        level1_files = []
        for level1 in level1_dataset:
            level1_filename = level1['uri']
            level1_files.append(level1_filename)
            if '-PRO' in level1_filename:
                pro_files = True
            if '-EPI' in level1_filename:
                epi_files = True

        if not pro_files:
            LOG.warning("PRO file is missing...")
            return False
        if not epi_files:
            LOG.warning("EPI file is missing...")
            return False

        self.level1_files = level1_files
        self.l1c_result = self.pool.apply_async(process_one_scan, (self.level1_files,
                                                                   self.working_home))
        return True


def publish_l1c(publisher, result_file, mda, **kwargs):
    """Publish the messages that l1c files are ready."""

    if not result_file:
        return

    # Now publish:
    to_send = mda.copy()
    # Delete the input level-1 dataset from the message:
    try:
        del(to_send['dataset'])
    except KeyError:
        LOG.warning("Couldn't remove dataset from message")

    if ('orbit' in kwargs) and ('orbit_number' in to_send.keys()):
        to_send["orig_orbit_number"] = to_send["orbit_number"]
        to_send["orbit_number"] = kwargs['orbit']

    to_send["uri"] = urlunsplit(('ssh', socket.gethostname(), result_file, '', ''))
    filename = os.path.basename(result_file)
    to_send["uid"] = filename

    publish_topic = kwargs.get('publish_topic', 'Unknown')
    site = kwargs.get('site', 'unknown')

    to_send['format'] = 'PPS-L1C'
    to_send['type'] = 'NETCDF'
    to_send['data_processing_level'] = '1c'
    to_send['site'] = site

    LOG.debug('Site = %s', site)
    LOG.debug('Publish topic = %s', publish_topic)
    for topic in publish_topic:
        msg = Message('/'.join(('', topic)),
                      "file", to_send).encode()

    LOG.debug("sending: %s", str(msg))
    publisher.send(msg)


def seviri_l1c_runner(options, service_name="unknown"):
    """The live runner for the SEVIRI l1c product generation."""

    LOG.info("Start the SEVIRI l1C runner...")
    LOG.debug("Listens for messages of type: %s", str(options['message_types']))

    ncpus_available = cpu_count()
    LOG.info("Number of CPUs available = %s", str(ncpus_available))
    ncpus = int(options.get('num_of_cpus', 1))
    LOG.info("Will use %d CPUs when running the CSPP SEVIRI instances", ncpus)

    l1c_proc = L1cProcessor(ncpus)
    with Subscribe('', options['message_types'], True) as sub:
        with Publish('seviri_l1c_runner', 0) as publisher:
            while True:
                #                 count = 0
                for msg in sub.recv():
                    l1c_proc.initialise(service_name)
                    status = l1c_proc.run(msg)
                    if not status:
                        break  # end the loop and reinitialize !
                    LOG.debug(
                        "Received message data = %s", str(l1c_proc.message_data))
                    LOG.info("Get the results from the multiptocessing pool-run")

                    l1c_proc.l1cfile = l1c_proc.l1c_result.get()
                    l1c_filepaths = l1c_proc.deliver_output_file()
                    if l1c_proc.result_home == l1c_proc.working_home:
                        LOG.info("home_dir = working_dir no cleaning necessary")
                    else:
                        LOG.info("Cleaning up directory %s", l1c_proc.working_home)
                        cleanup_workdir(l1c_proc.working_home + '/')

                    publish_l1c(publisher, l1c_filepaths,
                                l1c_proc.message_data,
                                orbit=l1c_proc.orbit_number,
                                publish_topic=l1c_proc.publish_topic,
                                environment=l1c_proc.environment,
                                site=l1c_proc.site)
                    LOG.info("L1C processing has completed.")


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
                        default='seviri_l1c_config.yaml',
                        help="The file containing " +
                        "configuration parameters e.g. product_filter_config.yaml, \n" +
                        "default = ./seviri_l1c_config.yaml",
                        required=True)
    parser.add_argument("-s", "--service",
                        type=str,
                        dest="service",
                        default='seviri-l1c',
                        help="Name of the service (e.g. iasi-lvl2), \n" +
                        "default = seviri-l1c")
    parser.add_argument("-m", "--mode",
                        type=str,
                        dest="mode",
                        default='utv',
                        help="Environment. replaces SMHI_MODE \n" +
                        "default = utv")
    parser.add_argument("-v", "--verbose", dest="verbosity", action="count", default=0,
                        help="Verbosity (between 1 and 2 occurrences with more leading to more "
                        "verbose logging). WARN=0, INFO=1, "
                        "DEBUG=2. This is overridden by the log config file if specified.")

    args = parser.parse_args()
    setup_logging(args)

    service = args.service.lower()

    if 'template' in args.config_file:
        LOG.error("Template file given as master config, aborting!")
        sys.exit()

    return service, args.config_file, args.mode


if __name__ == '__main__':

    (SERVICE_NAME, CONFIG_FILENAME, ENVIRON) = get_arguments()

    OPTIONS = get_config(CONFIG_FILENAME, SERVICE_NAME, ENVIRON)

    OPTIONS['environment'] = ENVIRON
    OPTIONS['nagios_config_file'] = None

    seviri_l1c_runner(OPTIONS, SERVICE_NAME)
