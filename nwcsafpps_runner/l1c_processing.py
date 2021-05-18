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

"""The level-1c processing tools
"""

import logging
from multiprocessing import cpu_count
from multiprocessing.pool import ThreadPool
from level1c4pps.seviri2pps_lib import process_one_scan
from nwcsafpps_runner.config import get_config_from_yamlfile as get_config

LOG = logging.getLogger(__name__)

SUPPORTED_METEOSAT_SATELLITES = ['meteosat-8', 'meteosat-9', 'meteosat-10', 'meteosat-11']


class L1cProcessor(object):
    """Container for the NWCSAF/PPS Level-c processing."""

    def __init__(self, config_filename, service_name):

        options = get_config(config_filename, service_name)

        self.subscribe_topics = options['message_types']
        LOG.debug("Listens for messages of type: %s", str(self.subscribe_topics))

        ncpus_available = cpu_count()
        LOG.info("Number of CPUs available = %s", str(ncpus_available))
        ncpus = int(options.get('num_of_cpus', 1))
        LOG.info("Will use %d CPUs when running the level-1c processing instances", ncpus)

        self.pool = ThreadPool(ncpus)

        self.sensor = "unknown"
        self.orbit_number = 99999  # Initialised orbit number
        self.platform_name = 'unknown'
        self.l1c_result = None
        self.pass_start_time = None
        self.l1cfile = None
        self.level1_files = []

        self.result_home = options.get('output_dir', '/tmp')
        self.publish_topic = options.get('publish_topic')
        self.message_data = None
        self.service = None

    def initialise(self, service):
        """Initialise the processor."""
        self.l1c_result = None
        self.pass_start_time = None
        self.l1cfile = None
        self.level1_files = []
        self.service = service

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
                                                                   self.result_home))
        return True
