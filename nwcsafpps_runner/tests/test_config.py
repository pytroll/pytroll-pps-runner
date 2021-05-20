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

"""Unit testing the config handling.
"""

from unittest.mock import patch
import unittest
import yaml

from nwcsafpps_runner.config import get_config_from_yamlfile as get_config


TEST_YAML_CONTENT_OK = """
seviri-l1c:
  message_types: [/1b/hrit/0deg]
  publish_topic: [1c/nc/0deg]
  instrument: 'seviri'
  num_of_cpus: 2

  output_dir: /san1/geo_in/lvl1c
"""


def create_config_from_yaml(yaml_content_str):
    """Create aapp-runner config dict from a yaml file."""
    return yaml.load(yaml_content_str, Loader=yaml.FullLoader)


class TestGetConfig(unittest.TestCase):
    """Test getting the yaml config from file"""

    def setUp(self):
        self.config_complete = create_config_from_yaml(TEST_YAML_CONTENT_OK)

    @patch('nwcsafpps_runner.config.load_config_from_file')
    def test_read_config(self, config):
        """Test loading and initialising the yaml config"""
        config.return_value = self.config_complete
        myconfig_filename = '/tmp/my/config/file'

        result = get_config(myconfig_filename, 'seviri-l1c')

        expected = {'message_types': ['/1b/hrit/0deg'],
                    'publish_topic': ['1c/nc/0deg'],
                    'instrument': 'seviri',
                    'num_of_cpus': 2,
                    'output_dir': '/san1/geo_in/lvl1c'}

        self.assertDictEqual(result, expected)
