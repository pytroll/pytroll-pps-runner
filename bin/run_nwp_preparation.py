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
import time

from datetime import datetime, timedelta
from nwcsafpps_runner.logger import setup_logging
from nwcsafpps_runner.prepare_nwp import update_nwp
from nwcsafpps_runner.utils import NwpPrepareError

NWP_FLENS = [6, 9, 12, 15, 18, 21, 24]

LOG = logging.getLogger('nwp-preparation')

# datetime.datetime.utcnow => datetime.datetime.now(datetime.UTC) ~python 3.12

def prepare_nwp4pps(options, flens):
    """Prepare NWP data for pps."""

    config_file_name = options.config_file
    every_hour_minute = options.run_every_hour_at_minute
    starttime = datetime.utcnow() - timedelta(days=1)
    LOG.info("Preparing nwp for PPS")
    update_nwp(starttime, flens, config_file_name)
    if every_hour_minute > 60:
        return
    while True:
        minute = datetime.utcnow().minute
        if minute < every_hour_minute or minute > every_hour_minute + 7:
            LOG.info("Not time for nwp preparation for pps yet, waiting 5 minutes")
            time.sleep(60 * 5)
        else:
            starttime = datetime.utcnow() - timedelta(days=1)
            LOG.info("Preparing nwp for PPS")
            try:
                update_nwp(starttime, flens, config_file_name)
            except (NwpPrepareError, IOError):
                LOG.exception("Something went wrong in update_nwp...")
                raise
            LOG.info("Ready with nwp preparation for pps, waiting 45 minutes")
            time.sleep(45 * 60)


def get_arguments():
    """Get command line arguments."""
    parser = argparse.ArgumentParser()

    parser.add_argument("-l", "--log-config",
                        help="Log config file to use instead of the standard logging.")
    parser.add_argument('-c', '--config_file',
                        type=str,
                        dest='config_file',
                        default='pps_nwp_config.yaml',
                        help="The file containing " +
                        "configuration parameters e.g. pps_nwp_config.yaml, \n" +
                        "default = ./pps_nwp_config.yaml",
                        required=True)
    parser.add_argument('--run_every_hour_at_minute',
                        type=int,
                        default='99',
                        help="Rerun preparation every hour approximately at this minute.",
                        required=False)
    parser.add_argument("-v", "--verbose", dest="verbosity", action="count", default=0,
                        help="Verbosity (between 1 and 2 occurrences with more leading to more "
                        "verbose logging). WARN=0, INFO=1, "
                        "DEBUG=2. This is overridden by the log config file if specified.")

    args = parser.parse_args()

    if 'template' in args.config_file:
        raise IOError("Template file given as master config, aborting!")
    return args


if __name__ == '__main__':

    options = get_arguments()
    setup_logging(options)
    prepare_nwp4pps(options, NWP_FLENS)
