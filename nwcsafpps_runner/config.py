#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2018 Adam.Dybbroe

# Author(s):

#   Adam.Dybbroe <a000680@c20671.ad.smhi.se>

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

"""
"""

import os
import socket
#: Python 2/3 differences
import six
if six.PY2:
    import ConfigParser
elif six.PY3:
    import configparser as ConfigParser

MODE = os.getenv("SMHI_MODE")
if MODE is None:
    MODE = "offline"

CONFIG_PATH = os.environ.get('PPSRUNNER_CONFIG_DIR', './')

LVL1_NPP_PATH = os.environ.get('LVL1_NPP_PATH', None)
LVL1_EOS_PATH = os.environ.get('LVL1_EOS_PATH', None)


def get_config(configfile):

    conf = ConfigParser.ConfigParser()
    conf.read(os.path.join(CONFIG_PATH, configfile))

    options = {}
    for option, value in conf.items(MODE, raw=True):
        options[option] = value

    subscribe_topics = options.get('subscribe_topics').split(',')
    for item in subscribe_topics:
        if len(item) == 0:
            subscribe_topics.remove(item)

    options['subscribe_topics'] = subscribe_topics
    options['number_of_threads'] = int(options.get('number_of_threads', 5))
    options['maximum_pps_processing_time_in_minutes'] = int(options.get('maximum_pps_processing_time_in_minutes', 20))
    options['servername'] = options.get('servername', socket.gethostname())
    options['station'] = options.get('station', 'unknown')

    return options
