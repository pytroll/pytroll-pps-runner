#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2021 Pytroll Developers

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

"""The level-1c processing tools
"""

import logging
from six.moves.urllib.parse import urlparse
from multiprocessing import cpu_count
from multiprocessing.pool import ThreadPool
from level1c4pps.seviri2pps_lib import process_one_scan as process_seviri
from level1c4pps.viirs2pps_lib import process_one_scene as process_viirs
from level1c4pps.modis2pps_lib import process_one_scene as process_modis
from level1c4pps.avhrr2pps_lib import process_one_scene as process_avhrr

from nwcsafpps_runner.config import get_config_from_yamlfile as get_config

LOG = logging.getLogger(__name__)

SUPPORTED_SERVICE_NAMES = ['seviri-l1c', 'viirs-l1c', 'avhrr-l1c', 'modis-l1c']

SUPPORTED_SATELLITES = {'seviri-l1c':
                        ['meteosat-8', 'meteosat-9', 'meteosat-10', 'meteosat-11'],
                        'viirs-l1c': ['suomi-npp', 'noaa-20', 'noaa-21'],
                        'avhrr-l1c': ['noaa-19', 'noaa-18'],
                        'modis-l1c': ['eos-terra', 'eos-aqua']
                        }

LVL1C_PROCESSOR_MAPPING = {'seviri-l1c': process_seviri,
                           'viirs-l1c': process_viirs,
                           'modis-l1c': process_modis,
                           'avhrr-l1c': process_avhrr}


class ServiceNameNotSupported(Exception):
    pass


class MessageTypeNotSupported(Exception):
    pass


class MessageContentMissing(Exception):
    pass


class PlatformNameInconsistentWithService(Exception):
    pass


class DatasetIsEmpty(Exception):
    pass


class L1cProcessor(object):
    """Container for the NWCSAF/PPS Level-c processing."""

    def __init__(self, config_filename, service_name):

        options = get_config(config_filename, service_name)

        self.initialize(service_name)
        self._l1c_processor_call_kwargs = options.get('l1cprocess_call_arguments', {})

        self.subscribe_topics = options['message_types']
        LOG.debug("Listens for messages of type: %s", str(self.subscribe_topics))

        ncpus_available = cpu_count()
        LOG.info("Number of CPUs available = %s", str(ncpus_available))
        ncpus = int(options.get('num_of_cpus', 1))
        LOG.info("Will use %d CPUs when running the level-1c processing instances", ncpus)

        self.pool = ThreadPool(ncpus)

        self.sensor = "unknown"
        self.orbit_number = 99999  # Initialized orbit number
        self.platform_name = 'unknown'

        self.result_home = options.get('output_dir', '/tmp')
        self.publish_topic = options.get('publish_topic')
        self.message_data = None
        self.nameservers = options.get('nameservers')
        if self.nameservers is not None and not isinstance(self.nameservers, list):
            self.nameservers = [self.nameservers]
        self.orbit_number_from_msg = options.get('orbit_number_from_msg', False)

    def initialize(self, service):
        """Initialize the processor."""
        check_service_is_supported(service)
        self.l1c_result = None
        self.pass_start_time = None
        self.l1cfile = None
        self.level1_files = []
        self.service = service

    def run(self, msg):
        """Start the L1c processing using the relevant sensor specific function from level1c4pps."""

        check_message_okay(msg)

        self.platform_name = str(msg.data['platform_name'])
        self.check_platform_name_consistent_with_service()

        self.sensor = str(msg.data['sensor'])
        self.message_data = self._get_message_data(msg)

        level1_dataset = self.message_data.get('dataset')

        if self.orbit_number_from_msg:
            if 'orbit_number' in self.message_data:
                self.orbit_number = int(self.message_data.get('orbit_number'))
            else:
                LOG.warning("You asked for orbit_number from the message, but its not there. Keep init orbit.")

        if len(level1_dataset) < 1:
            raise DatasetIsEmpty('No level-1 data in dataset!')

        self.get_level1_files_from_dataset(level1_dataset)

        l1c_proc = LVL1C_PROCESSOR_MAPPING.get(self.service)
        if not l1c_proc:
            raise AttributeError("Could not find suitable level-1c processor! Service = %s" % self.service)

        self.l1c_result = self.pool.apply_async(l1c_proc, (self.level1_files,
                                                           self.result_home),
                                                self._l1c_processor_call_kwargs)

    def _get_message_data(self, message):
        """Return the data dict in the Posttroll message."""
        return message.data

    def get_level1_files_from_dataset(self, level1_dataset):
        """Get the level-1 files from the dataset."""

        if self.service in ['seviri-l1c']:
            self.level1_files = get_seviri_level1_files_from_dataset(level1_dataset)
        else:
            for level1 in level1_dataset:
                self.level1_files.append(urlparse(level1['uri']).path)

    def check_platform_name_consistent_with_service(self):
        """Check that the platform name is consistent with the service name."""

        if self.platform_name.lower() not in SUPPORTED_SATELLITES.get(self.service, []):
            errmsg = ("%s: Platform name not supported for this service: %s",
                      str(self.platform_name), self.service)
            raise PlatformNameInconsistentWithService(errmsg)


def get_seviri_level1_files_from_dataset(level1_dataset):
    """Get the seviri level-1 filenames from the dataset and return as list."""

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
        return []
    if not epi_files:
        LOG.warning("EPI file is missing...")
        return []

    return level1_files


def check_message_okay(msg):
    """Check that the message is okay and has the necessary fields."""
    if msg.type != 'dataset':
        raise MessageTypeNotSupported("Not a dataset, don't do anything...")

    if ('platform_name' not in msg.data):
        raise MessageContentMissing("Message is lacking crucial fields: platform_name")

    if ('start_time' not in msg.data):
        raise MessageContentMissing("Message is lacking crucial fields: start_time")


def check_service_is_supported(service_name):
    """Check that the service is supported."""
    if service_name not in SUPPORTED_SERVICE_NAMES:
        errmsg = "Service name %s is not yet supported" % service_name
        raise ServiceNameNotSupported(errmsg)
