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

from datetime import datetime, timedelta
from nwcsafpps_runner.logger import setup_logging
from nwcsafpps_runner.prepare_nwp import update_nwp
from nwcsafpps_runner.utils import (NwpPrepareError)

NWP_FLENS = [6, 9, 12, 15, 18, 21, 24]

LOG = logging.getLogger('nwp-preparation')


def prepare_nwp4pps(config_file_name, flens):
    """Prepare NWP data for pps."""

    starttime = datetime.datetime.now(datetime.UTC) - timedelta(days=1)
    LOG.info("Preparing nwp for PPS")
    try:
        update_nwp(starttime, flens, config_file_name)
    except (NwpPrepareError, IOError):
        LOG.exception("Something went wrong in update_nwp...")
        raise
    LOG.info("Ready with nwp preparation for pps")


def get_arguments():
    """
    Get command line arguments.
    Return the config filepath
    """

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
    parser.add_argument("-v", "--verbose", dest="verbosity", action="count", default=0,
                        help="Verbosity (between 1 and 2 occurrences with more leading to more "
                        "verbose logging). WARN=0, INFO=1, "
                        "DEBUG=2. This is overridden by the log config file if specified.")

    args = parser.parse_args()
    setup_logging(args)

    if 'template' in args.config_file:
        raise IOError("Template file given as master config, aborting!")

    return args.config_file


if __name__ == '__main__':

    CONFIG_FILENAME = get_arguments()
    prepare_nwp4pps(CONFIG_FILENAME, NWP_FLENS)
