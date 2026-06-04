#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2018 - 2022 Pytroll Developers

# Author(s):

#   Adam.Dybbroe <Firstname.Lastname at smhi.se>

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

"""Utility functions for NWCSAF/pps runner(s)."""

import logging
import os
import shlex
import threading
from subprocess import PIPE, Popen


LOG = logging.getLogger(__name__)


class NwpPrepareError(Exception):
    pass


def run_command(cmdstr):
    """Run system command."""
    myargs = shlex.split(str(cmdstr))

    LOG.debug("Command: " + str(cmdstr))
    LOG.debug('Command sequence= ' + str(myargs))
    #: TODO: What is this
    try:
        proc = Popen(myargs, shell=False, stderr=PIPE, stdout=PIPE)
    except NwpPrepareError:
        LOG.exception("Failed when preparing NWP data for PPS...")

    out_reader = threading.Thread(
        target=logreader, args=(proc.stdout, LOG.info))
    err_reader = threading.Thread(
        target=logreader, args=(proc.stderr, LOG.info))
    out_reader.start()
    err_reader.start()
    out_reader.join()
    err_reader.join()

    return proc.wait()


def create_pps_file_from_lvl1c(l1c_file_name, pps_control_path, name_tag, file_type):
    """From lvl1c file create name_tag-file of type file_type."""
    from trollsift import compose, parse
    f_pattern = 'S_NWC_{name_tag}_{platform_id}_{orbit_number}_{start_time}Z_{end_time}Z{file_type}'
    l1c_path, l1c_file = os.path.split(l1c_file_name)
    data = parse(f_pattern, l1c_file)
    data["name_tag"] = name_tag
    data["file_type"] = file_type
    return os.path.join(pps_control_path, compose(f_pattern, data))


def logreader(stream, log_func):
    while True:
        mystring = stream.readline()
        if not mystring:
            break
        log_func(mystring.strip())
    stream.close()
