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

CONFIG_PATH = os.environ.get('PPSRUNNER_CONFIG_DIR', './')
CONFIG_FILE = os.environ.get('PPSRUNNER_CONFIG_FILE', 'pps2018_config.yaml')


def load_config_from_file(filepath):
    """Load the yaml config from file, given the file-path"""
    with open(filepath, 'r') as fp_:
        config = yaml.load(fp_, Loader=yaml.FullLoader)

    return config


def get_config_from_yamlfile(configfile, service):
    """Get the configuration from file."""

    config = load_config_from_file(configfile)

    options = {}
    for item in config:
        if not isinstance(config[item], dict):
            options[item] = config[item]
        elif item not in [service]:
            continue
        for key in config[service]:
            options[key] = config[service][key]

    return options


def get_config(conf, service=''):
    """Get configuration from file using the env for the file-path."""
    configfile = os.path.join(CONFIG_PATH, conf)
    filetype = os.path.splitext(conf)[1]
    if filetype == '.yaml':
        options = get_config_yaml(configfile, service)
    else:
        print("%s is not a valid extension for the config file" % filetype)
        print("Pleas use .yaml")
        options = -1
    return options


def get_config_yaml(configfile, service=''):
    """Get the configuration from file."""
    config = load_config_from_file(configfile)

    options = {}
    for item in config:
        if not isinstance(config[item], dict) or item not in service:
            options[item] = config[item]
        elif item in [service]:
            for key in config[service]:
                if not isinstance(config[service][key], dict):
                    options[key] = config[service][key]

    if isinstance(options.get('subscribe_topics'), str):
        subscribe_topics = options.get('subscribe_topics').split(',')
        for item in subscribe_topics:
            if len(item) == 0:
                subscribe_topics.remove(item)
        options['subscribe_topics'] = subscribe_topics

    options['number_of_threads'] = int(options.get('number_of_threads', 5))
    options['maximum_pps_processing_time_in_minutes'] = int(options.get('maximum_pps_processing_time_in_minutes', 20))
    options['servername'] = options.get('servername', socket.gethostname())
    options['station'] = options.get('station', 'unknown')
    options['run_cmask_prob'] = options.get('run_cmask_prob', True)
    options['run_pps_cpp'] = options.get('run_pps_cpp', True)

    return options
