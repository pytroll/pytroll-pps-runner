#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2019 - 2021 Pytroll

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
"""Collector to add PPS level1c file to message with PPS files."""

import argparse
import logging

from nwcsafpps_runner.logger import setup_logging
from nwcsafpps_runner.pps_collector_lib import pps_collector_runner


LOOP = True

LOG = logging.getLogger('pps-l1c-collector')


def get_arguments():
    """Get command line arguments."""
    parser = argparse.ArgumentParser()

    parser.add_argument("-l", "--log-config",
                        help="Log config file to use instead of the standard logging.")
    parser.add_argument('-c', '--config_file',
                        type=str,
                        dest='config_file',
                        default='config.yaml',
                        help="The file containing " +
                        "configuration parameters, \n" +
                        "default = ./config.yaml",
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

    config_file = get_arguments()
    pps_collector_runner(config_file)
