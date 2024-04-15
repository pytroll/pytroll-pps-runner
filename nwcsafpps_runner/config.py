#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2018 - 2021 Pytroll Developers

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

"""Reading configuration settings for NWCSAF/pps runner(s).
"""

import os
import socket

import yaml


def load_config_from_file(filepath):
    """Load the yaml config from file, given the file-path"""
    with open(filepath, 'r') as fp_:
        config = yaml.load(fp_, Loader=yaml.FullLoader)
    return config


def move_service_dict_attributes_to_top_level(options, service):
    if service in options and isinstance(options[service], dict):
        service_config = options.pop(service)
        for key in service_config:
            options[key] = service_config[key]


def get_config(configfile, add_defaults=False, service=None):
    """Get configuration from the config file."""
    filetype = os.path.splitext(configfile)[1]
    if filetype != '.yaml':
        raise ValueError("Configfile {:s} should be of type .yaml, not {:s}".format(configfile,
                                                                                    filetype))
    options = load_config_from_file(configfile)
    modify_config_vars(options)
    if add_defaults:
        add_some_default_vars(options)
    if service is not None:
        move_service_dict_attributes_to_top_level(options, service)
    return options


def modify_config_vars(options):
    """Modify some config options."""
    if isinstance(options.get('subscribe_topics'), str):
        subscribe_topics = options.get('subscribe_topics').split(',')
        for item in subscribe_topics:
            if len(item) == 0:
                subscribe_topics.remove(item)
        options['subscribe_topics'] = subscribe_topics


def add_some_default_vars(options):
    """Add some default vars."""
    # service = '', probably no items are '' so this is the same as:
    options['number_of_threads'] = int(options.get('number_of_threads', 5))
    options['maximum_pps_processing_time_in_minutes'] = int(options.get('maximum_pps_processing_time_in_minutes', 20))
    options['servername'] = options.get('servername', socket.gethostname())
    options['station'] = options.get('station', 'unknown')
    options['run_cmask_prob'] = options.get('run_cmask_prob', True)
