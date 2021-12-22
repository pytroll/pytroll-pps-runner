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

"""Unit testing the level-1c runner code
"""

import pytest
from unittest.mock import patch
import unittest
from posttroll.message import Message
from datetime import datetime
import yaml
import tempfile

from nwcsafpps_runner.message_utils import publish_l1c, prepare_l1c_message
from nwcsafpps_runner.l1c_processing import check_message_okay
from nwcsafpps_runner.l1c_processing import check_service_is_supported
from nwcsafpps_runner.l1c_processing import L1cProcessor
from nwcsafpps_runner.l1c_processing import ServiceNameNotSupported
from nwcsafpps_runner.l1c_processing import MessageTypeNotSupported
from nwcsafpps_runner.l1c_processing import MessageContentMissing


TEST_YAML_CONTENT_OK = """
seviri-l1c:
  message_types: [/1b/hrit/0deg]
  publish_topic: [/1c/nc/0deg]
  instrument: 'seviri'
  num_of_cpus: 2

  output_dir: /san1/geo_in/lvl1c

  l1cprocess_call_arguments:
    engine: 'netcdf4'
    rotate: True
"""
TEST_YAML_CONTENT_OK_MINIMAL = """
seviri-l1c:
  message_types: [/1b/hrit/0deg]
  publish_topic: [/1c/nc/0deg]
  instrument: 'seviri'
"""

TEST_YAML_CONTENT_VIIRS_OK = """
viirs-l1c:
  message_types: [/segment/SDR/1B]
  publish_topic: [/segment/SDR/1C]
  instrument: 'viirs'
  num_of_cpus: 2

  output_dir: /san1/polar_in/lvl1c
"""

TEST_YAML_CONTENT_VIIRS_ORBIT_NUMBER_FROM_MSG_OK = """
viirs-l1c:
  message_types: [/segment/SDR/1B]
  publish_topic: [/segment/SDR/1C]
  instrument: 'viirs'
  num_of_cpus: 2

  output_dir: /san1/polar_in/lvl1c
  orbit_number_from_msg: True
"""

TEST_YAML_CONTENT_NAMESERVERS_OK = """
seviri-l1c:
  message_types: [/1b/hrit/0deg]
  publish_topic: [/1c/nc/0deg]
  instrument: 'seviri'
  num_of_cpus: 2

  output_dir: /san1/geo_in/lvl1c

  l1cprocess_call_arguments:
    engine: 'netcdf4'
    rotate: True
  nameservers:
    - 'test.nameserver'
"""

TEST_INPUT_MSG = """pytroll://1b/hrit/0deg dataset safusr.u@lxserv1043.smhi.se 2021-05-18T14:28:54.154172 v1.01 application/json {"data_type": "MSG4", "orig_platform_name": "MSG4", "start_time": "2021-05-18T14:15:00", "variant": "0DEG", "series": "MSG4", "platform_name": "Meteosat-11", "channel": "", "nominal_time": "2021-05-18T14:15:00", "compressed": "", "origin": "172.18.0.248:9093", "dataset": [{"uri": "/san1/geo_in/0deg/H-000-MSG4__-MSG4________-_________-PRO______-202105181415-__", "uid": "H-000-MSG4__-MSG4________-_________-PRO______-202105181415-__"}, {"uri": "/san1/geo_in/0deg/H-000-MSG4__-MSG4________-HRV______-000001___-202105181415-__", "uid": "H-000-MSG4__-MSG4________-HRV______-000001___-202105181415-__"}], "sensor": ["seviri"]}"""

TEST_INPUT_MSG_NO_DATASET = """pytroll://1b/hrit/0deg file safusr.u@lxserv1043.smhi.se 2021-05-18T14:28:54.154172 v1.01 application/json {"data_type": "MSG4", "orig_platform_name": "MSG4", "start_time": "2021-05-18T14:15:00", "variant": "0DEG", "series": "MSG4", "platform_name": "Meteosat-11", "channel": "", "nominal_time": "2021-05-18T14:15:00", "compressed": "", "origin": "172.18.0.248:9093", "file": "/san1/geo_in/0deg/H-000-MSG4__-MSG4________-_________-PRO______-202105181415-__", "sensor": ["seviri"]}"""


TEST_INPUT_MSG_NO_PLATFORM_NAME = """pytroll://1b/hrit/0deg dataset safusr.u@lxserv1043.smhi.se 2021-05-18T14:28:54.154172 v1.01 application/json {"data_type": "MSG4", "orig_platform_name": "MSG4", "start_time": "2021-05-18T14:15:00", "variant": "0DEG", "series": "MSG4", "channel": "", "nominal_time": "2021-05-18T14:15:00", "compressed": "", "origin": "172.18.0.248:9093", "dataset": [{"uri": "/san1/geo_in/0deg/H-000-MSG4__-MSG4________-_________-PRO______-202105181415-__", "uid": "H-000-MSG4__-MSG4________-_________-PRO______-202105181415-__"}, {"uri": "/san1/geo_in/0deg/H-000-MSG4__-MSG4________-HRV______-000001___-202105181415-__", "uid": "H-000-MSG4__-MSG4________-HRV______-000001___-202105181415-__"}], "sensor": ["seviri"]}"""

TEST_INPUT_MSG_NO_START_TIME = """pytroll://1b/hrit/0deg dataset safusr.u@lxserv1043.smhi.se 2021-05-18T14:28:54.154172 v1.01 application/json {"data_type": "MSG4", "orig_platform_name": "MSG4", "variant": "0DEG", "series": "MSG4", "platform_name": "Meteosat-11", "channel": "", "nominal_time": "2021-05-18T14:15:00", "compressed": "", "origin": "172.18.0.248:9093", "dataset": [{"uri": "/san1/geo_in/0deg/H-000-MSG4__-MSG4________-_________-PRO______-202105181415-__", "uid": "H-000-MSG4__-MSG4________-_________-PRO______-202105181415-__"}, {"uri": "/san1/geo_in/0deg/H-000-MSG4__-MSG4________-HRV______-000001___-202105181415-__", "uid": "H-000-MSG4__-MSG4________-HRV______-000001___-202105181415-__"}], "sensor": ["seviri"]}"""


TEST_VIIRS_MSG_DATA = {'start_time': datetime(2021, 6, 1, 5, 43, 11, 100000), 'end_time': datetime(2021, 6, 1, 5, 44, 35, 300000), 'orbit_number': 49711, 'platform_name': 'Suomi-NPP', 'sensor': 'viirs', 'format': 'SDR', 'type': 'HDF5', 'data_processing_level': '1B', 'variant': 'DR', 'orig_orbit_number': 49710, 'dataset': [{'uri': 'ssh://lxserv1043.smhi.se/san1/polar_in/direct_readout/npp/lvl1/npp_20210601_0536_49711/GMODO_npp_d20210601_t0543111_e0544353_b49711_c20210601055246487163_cspp_dev.h5', 'uid': 'GMODO_npp_d20210601_t0543111_e0544353_b49711_c20210601055246487163_cspp_dev.h5'}, {'uri': 'ssh://lxserv1043.smhi.se/san1/polar_in/direct_readout/npp/lvl1/npp_20210601_0536_49711/GMTCO_npp_d20210601_t0543111_e0544353_b49711_c20210601055246379744_cspp_dev.h5', 'uid': 'GMTCO_npp_d20210601_t0543111_e0544353_b49711_c20210601055246379744_cspp_dev.h5'}, {'uri': 'ssh://lxserv1043.smhi.se/san1/polar_in/direct_readout/npp/lvl1/npp_20210601_0536_49711/SVM01_npp_d20210601_t0543111_e0544353_b49711_c20210601055314738876_cspp_dev.h5', 'uid': 'SVM01_npp_d20210601_t0543111_e0544353_b49711_c20210601055314738876_cspp_dev.h5'}, {'uri': 'ssh://lxserv1043.smhi.se/san1/polar_in/direct_readout/npp/lvl1/npp_20210601_0536_49711/SVM02_npp_d20210601_t0543111_e0544353_b49711_c20210601055314768881_cspp_dev.h5', 'uid': 'SVM02_npp_d20210601_t0543111_e0544353_b49711_c20210601055314768881_cspp_dev.h5'}, {'uri': 'ssh://lxserv1043.smhi.se/san1/polar_in/direct_readout/npp/lvl1/npp_20210601_0536_49711/SVM03_npp_d20210601_t0543111_e0544353_b49711_c20210601055314798831_cspp_dev.h5', 'uid': 'SVM03_npp_d20210601_t0543111_e0544353_b49711_c20210601055314798831_cspp_dev.h5'}, {'uri': 'ssh://lxserv1043.smhi.se/san1/polar_in/direct_readout/npp/lvl1/npp_20210601_0536_49711/SVM04_npp_d20210601_t0543111_e0544353_b49711_c20210601055314834323_cspp_dev.h5', 'uid': 'SVM04_npp_d20210601_t0543111_e0544353_b49711_c20210601055314834323_cspp_dev.h5'}, {'uri': 'ssh://lxserv1043.smhi.se/san1/polar_in/direct_readout/npp/lvl1/npp_20210601_0536_49711/SVM05_npp_d20210601_t0543111_e0544353_b49711_c20210601055314869370_cspp_dev.h5', 'uid': 'SVM05_npp_d20210601_t0543111_e0544353_b49711_c20210601055314869370_cspp_dev.h5'}, {'uri': 'ssh://lxserv1043.smhi.se/san1/polar_in/direct_readout/npp/lvl1/npp_20210601_0536_49711/SVM06_npp_d20210601_t0543111_e0544353_b49711_c20210601055314903613_cspp_dev.h5', 'uid': 'SVM06_npp_d20210601_t0543111_e0544353_b49711_c20210601055314903613_cspp_dev.h5'}, {'uri': 'ssh://lxserv1043.smhi.se/san1/polar_in/direct_readout/npp/lvl1/npp_20210601_0536_49711/SVM07_npp_d20210601_t0543111_e0544353_b49711_c20210601055315994299_cspp_dev.h5', 'uid': 'SVM07_npp_d20210601_t0543111_e0544353_b49711_c20210601055315994299_cspp_dev.h5'}, {'uri': 'ssh://lxserv1043.smhi.se/san1/polar_in/direct_readout/npp/lvl1/npp_20210601_0536_49711/SVM08_npp_d20210601_t0543111_e0544353_b49711_c20210601055314968487_cspp_dev.h5', 'uid': 'SVM08_npp_d20210601_t0543111_e0544353_b49711_c20210601055314968487_cspp_dev.h5'}, {'uri': 'ssh://lxserv1043.smhi.se/san1/polar_in/direct_readout/npp/lvl1/npp_20210601_0536_49711/SVM09_npp_d20210601_t0543111_e0544353_b49711_c20210601055314998705_cspp_dev.h5', 'uid': 'SVM09_npp_d20210601_t0543111_e0544353_b49711_c20210601055314998705_cspp_dev.h5'}, {'uri': 'ssh://lxserv1043.smhi.se/san1/polar_in/direct_readout/npp/lvl1/npp_20210601_0536_49711/SVM10_npp_d20210601_t0543111_e0544353_b49711_c20210601055315028952_cspp_dev.h5', 'uid': 'SVM10_npp_d20210601_t0543111_e0544353_b49711_c20210601055315028952_cspp_dev.h5'}, {'uri': 'ssh://lxserv1043.smhi.se/san1/polar_in/direct_readout/npp/lvl1/npp_20210601_0536_49711/SVM11_npp_d20210601_t0543111_e0544353_b49711_c20210601055315058971_cspp_dev.h5', 'uid': 'SVM11_npp_d20210601_t0543111_e0544353_b49711_c20210601055315058971_cspp_dev.h5'}, {
    'uri': 'ssh://lxserv1043.smhi.se/san1/polar_in/direct_readout/npp/lvl1/npp_20210601_0536_49711/SVM12_npp_d20210601_t0543111_e0544353_b49711_c20210601055315089629_cspp_dev.h5', 'uid': 'SVM12_npp_d20210601_t0543111_e0544353_b49711_c20210601055315089629_cspp_dev.h5'}, {'uri': 'ssh://lxserv1043.smhi.se/san1/polar_in/direct_readout/npp/lvl1/npp_20210601_0536_49711/SVM13_npp_d20210601_t0543111_e0544353_b49711_c20210601055315115727_cspp_dev.h5', 'uid': 'SVM13_npp_d20210601_t0543111_e0544353_b49711_c20210601055315115727_cspp_dev.h5'}, {'uri': 'ssh://lxserv1043.smhi.se/san1/polar_in/direct_readout/npp/lvl1/npp_20210601_0536_49711/SVM14_npp_d20210601_t0543111_e0544353_b49711_c20210601055315160850_cspp_dev.h5', 'uid': 'SVM14_npp_d20210601_t0543111_e0544353_b49711_c20210601055315160850_cspp_dev.h5'}, {'uri': 'ssh://lxserv1043.smhi.se/san1/polar_in/direct_readout/npp/lvl1/npp_20210601_0536_49711/SVM15_npp_d20210601_t0543111_e0544353_b49711_c20210601055315191874_cspp_dev.h5', 'uid': 'SVM15_npp_d20210601_t0543111_e0544353_b49711_c20210601055315191874_cspp_dev.h5'}, {'uri': 'ssh://lxserv1043.smhi.se/san1/polar_in/direct_readout/npp/lvl1/npp_20210601_0536_49711/SVM16_npp_d20210601_t0543111_e0544353_b49711_c20210601055315222054_cspp_dev.h5', 'uid': 'SVM16_npp_d20210601_t0543111_e0544353_b49711_c20210601055315222054_cspp_dev.h5'}, {'uri': 'ssh://lxserv1043.smhi.se/san1/polar_in/direct_readout/npp/lvl1/npp_20210601_0536_49711/GIMGO_npp_d20210601_t0543111_e0544353_b49711_c20210601055245783942_cspp_dev.h5', 'uid': 'GIMGO_npp_d20210601_t0543111_e0544353_b49711_c20210601055245783942_cspp_dev.h5'}, {'uri': 'ssh://lxserv1043.smhi.se/san1/polar_in/direct_readout/npp/lvl1/npp_20210601_0536_49711/GITCO_npp_d20210601_t0543111_e0544353_b49711_c20210601055245203321_cspp_dev.h5', 'uid': 'GITCO_npp_d20210601_t0543111_e0544353_b49711_c20210601055245203321_cspp_dev.h5'}, {'uri': 'ssh://lxserv1043.smhi.se/san1/polar_in/direct_readout/npp/lvl1/npp_20210601_0536_49711/SVI01_npp_d20210601_t0543111_e0544353_b49711_c20210601055314313242_cspp_dev.h5', 'uid': 'SVI01_npp_d20210601_t0543111_e0544353_b49711_c20210601055314313242_cspp_dev.h5'}, {'uri': 'ssh://lxserv1043.smhi.se/san1/polar_in/direct_readout/npp/lvl1/npp_20210601_0536_49711/SVI02_npp_d20210601_t0543111_e0544353_b49711_c20210601055314399749_cspp_dev.h5', 'uid': 'SVI02_npp_d20210601_t0543111_e0544353_b49711_c20210601055314399749_cspp_dev.h5'}, {'uri': 'ssh://lxserv1043.smhi.se/san1/polar_in/direct_readout/npp/lvl1/npp_20210601_0536_49711/SVI03_npp_d20210601_t0543111_e0544353_b49711_c20210601055314485227_cspp_dev.h5', 'uid': 'SVI03_npp_d20210601_t0543111_e0544353_b49711_c20210601055314485227_cspp_dev.h5'}, {'uri': 'ssh://lxserv1043.smhi.se/san1/polar_in/direct_readout/npp/lvl1/npp_20210601_0536_49711/SVI04_npp_d20210601_t0543111_e0544353_b49711_c20210601055314569515_cspp_dev.h5', 'uid': 'SVI04_npp_d20210601_t0543111_e0544353_b49711_c20210601055314569515_cspp_dev.h5'}, {'uri': 'ssh://lxserv1043.smhi.se/san1/polar_in/direct_readout/npp/lvl1/npp_20210601_0536_49711/SVI05_npp_d20210601_t0543111_e0544353_b49711_c20210601055314653190_cspp_dev.h5', 'uid': 'SVI05_npp_d20210601_t0543111_e0544353_b49711_c20210601055314653190_cspp_dev.h5'}, {'uri': 'ssh://lxserv1043.smhi.se/san1/polar_in/direct_readout/npp/lvl1/npp_20210601_0536_49711/GDNBO_npp_d20210601_t0543111_e0544353_b49711_c20210601055245009217_cspp_dev.h5', 'uid': 'GDNBO_npp_d20210601_t0543111_e0544353_b49711_c20210601055245009217_cspp_dev.h5'}, {'uri': 'ssh://lxserv1043.smhi.se/san1/polar_in/direct_readout/npp/lvl1/npp_20210601_0536_49711/SVDNB_npp_d20210601_t0543111_e0544353_b49711_c20210601055314094964_cspp_dev.h5', 'uid': 'SVDNB_npp_d20210601_t0543111_e0544353_b49711_c20210601055314094964_cspp_dev.h5'}, {'uri': 'ssh://lxserv1043.smhi.se/san1/polar_in/direct_readout/npp/lvl1/npp_20210601_0536_49711/IVCDB_npp_d20210601_t0543111_e0544353_b49711_c20210601055314133571_cspu_pop.h5', 'uid': 'IVCDB_npp_d20210601_t0543111_e0544353_b49711_c20210601055314133571_cspu_pop.h5'}]}

TEST_INPUT_MESSAGE_VIIRS_MSG = """pytroll://1b/viirs dataset safusr.u@lxserv1043.smhi.se 2021-05-18T14:28:54.154172 v1.01 application/json {"orbit_number": 49711, "data_type": "MSG4", "orig_platform_name": "MSG4", "start_time": "2021-05-18T14:15:00", "variant": "0DEG", "series": "MSG4", "platform_name": "Suomi-NPP", "channel": "", "nominal_time": "2021-05-18T14:15:00", "compressed": "", "origin": "172.18.0.248:9093", "dataset": [{"uri": "/san1/geo_in/0deg/H-000-MSG4__-MSG4________-_________-PRO______-202105181415-__", "uid": "H-000-MSG4__-MSG4________-_________-PRO______-202105181415-__"}, {"uri": "/san1/geo_in/0deg/H-000-MSG4__-MSG4________-HRV______-000001___-202105181415-__", "uid": "H-000-MSG4__-MSG4________-HRV______-000001___-202105181415-__"}], "sensor": ["seviri"]}"""

TEST_INPUT_MESSAGE_VIIRS_NO_ORBIT_MSG = """pytroll://1b/viirs dataset safusr.u@lxserv1043.smhi.se 2021-05-18T14:28:54.154172 v1.01 application/json {"data_type": "MSG4", "orig_platform_name": "MSG4", "start_time": "2021-05-18T14:15:00", "variant": "0DEG", "series": "MSG4", "platform_name": "Suomi-NPP", "channel": "", "nominal_time": "2021-05-18T14:15:00", "compressed": "", "origin": "172.18.0.248:9093", "dataset": [{"uri": "/san1/geo_in/0deg/H-000-MSG4__-MSG4________-_________-PRO______-202105181415-__", "uid": "H-000-MSG4__-MSG4________-_________-PRO______-202105181415-__"}, {"uri": "/san1/geo_in/0deg/H-000-MSG4__-MSG4________-HRV______-000001___-202105181415-__", "uid": "H-000-MSG4__-MSG4________-HRV______-000001___-202105181415-__"}], "sensor": ["seviri"]}"""


class MyFakePublisher(object):

    def __init__(self):
        pass

    def send(self, message):
        pass


def create_config_from_yaml(yaml_content_str):
    """Create aapp-runner config dict from a yaml file."""
    return yaml.load(yaml_content_str, Loader=yaml.FullLoader)


class TestPublishMessage(unittest.TestCase):

    @patch('nwcsafpps_runner.message_utils.socket.gethostname')
    def test_create_publish_message(self, gethostname):
        """Test the creation of the publish message."""

        gethostname.return_value = "my_local_server"
        my_fake_level1c_file = '/my/level1c/file/path/level1c.nc'
        input_msg = Message.decode(rawstr=TEST_INPUT_MSG)

        result = prepare_l1c_message(my_fake_level1c_file, input_msg.data, orbit=99999)

        expected = {'data_type': 'MSG4', 'orig_platform_name': 'MSG4',
                    'start_time': datetime(2021, 5, 18, 14, 15),
                    'variant': '0DEG', 'series': 'MSG4',
                    'platform_name': 'Meteosat-11',
                    'channel': '',
                    'nominal_time': datetime(2021, 5, 18, 14, 15),
                    'compressed': '',
                    'origin': '172.18.0.248:9093', 'sensor': ['seviri'],
                    'uri': 'ssh://my_local_server/my/level1c/file/path/level1c.nc',
                    'uid': 'level1c.nc',
                    'format': 'PPS-L1C',
                    'type': 'NETCDF',
                    'data_processing_level': '1c'}

        self.assertDictEqual(result, expected)

    @patch('nwcsafpps_runner.message_utils.Message.encode')
    def test_publish_messages(self, mock_message):
        """Test the sending the messages."""

        my_fake_publisher = MyFakePublisher()
        mock_message.return_value = "some pytroll message"

        pub_message = {'data_type': 'MSG4', 'orig_platform_name': 'MSG4',
                       'start_time': datetime(2021, 5, 18, 14, 15),
                       'variant': '0DEG', 'series': 'MSG4',
                       'platform_name': 'Meteosat-11',
                       'channel': '',
                       'nominal_time': datetime(2021, 5, 18, 14, 15),
                       'compressed': '',
                       'origin': '172.18.0.248:9093', 'sensor': ['seviri'],
                       'uri': 'ssh://my_local_server/my/level1c/file/path/level1c.nc',
                       'uid': 'level1c.nc',
                       'format': 'PPS-L1C',
                       'type': 'NETCDF',
                       'data_processing_level': '1c'}

        with patch.object(my_fake_publisher, 'send') as mock:
            publish_l1c(my_fake_publisher, pub_message, ['/1c/nc/0deg'])

        mock.assert_called_once()
        mock.assert_called_once_with("some pytroll message")


class TestL1cProcessing(unittest.TestCase):
    """Test the L1c processing module."""

    def setUp(self):
        self.config_complete = create_config_from_yaml(TEST_YAML_CONTENT_OK)
        self.config_minimum = create_config_from_yaml(TEST_YAML_CONTENT_OK_MINIMAL)
        self.config_viirs_ok = create_config_from_yaml(TEST_YAML_CONTENT_VIIRS_OK)
        self.config_complete_nameservers = create_config_from_yaml(TEST_YAML_CONTENT_NAMESERVERS_OK)
        self.config_viirs_orbit_number_from_msg_ok = create_config_from_yaml(
            TEST_YAML_CONTENT_VIIRS_ORBIT_NUMBER_FROM_MSG_OK)

    def test_check_service_is_supported(self):
        """Test the check for supported services."""
        check_service_is_supported('seviri-l1c')
        check_service_is_supported('viirs-l1c')
        check_service_is_supported('avhrr-l1c')
        check_service_is_supported('modis-l1c')

        self.assertRaises(ServiceNameNotSupported, check_service_is_supported, 'seviri')

        with pytest.raises(ServiceNameNotSupported) as exec_info:
            check_service_is_supported('avhrr')

        exception_raised = exec_info.value
        self.assertEqual('Service name avhrr is not yet supported', str(exception_raised))

    def test_check_message_okay_message_ok(self):
        """Test for check if message is okay."""
        input_msg = Message.decode(rawstr=TEST_INPUT_MSG)
        result = check_message_okay(input_msg)
        self.assertEqual(result, None)

    def test_check_message_okay_message_has_no_dataset(self):
        """Test that message is not okay if it is not a dataset."""
        input_msg = Message.decode(rawstr=TEST_INPUT_MSG_NO_DATASET)
        with pytest.raises(MessageTypeNotSupported) as exec_info:
            _ = check_message_okay(input_msg)

        exception_raised = exec_info.value
        self.assertEqual("Not a dataset, don't do anything...", str(exception_raised))

    def test_check_message_okay_message_has_no_platform_name(self):
        """Test that message is not okay if it does not contain platform_name."""
        input_msg = Message.decode(rawstr=TEST_INPUT_MSG_NO_PLATFORM_NAME)
        with pytest.raises(MessageContentMissing) as exec_info:
            _ = check_message_okay(input_msg)

        exception_raised = exec_info.value
        self.assertEqual("Message is lacking crucial fields: platform_name", str(exception_raised))

    def test_check_message_okay_message_has_no_start_time(self):
        """Test that message is not okay if it does not contain start_time."""
        input_msg = Message.decode(rawstr=TEST_INPUT_MSG_NO_START_TIME)
        with pytest.raises(MessageContentMissing) as exec_info:
            _ = check_message_okay(input_msg)

        exception_raised = exec_info.value
        self.assertEqual("Message is lacking crucial fields: start_time", str(exception_raised))

    @patch('nwcsafpps_runner.config.load_config_from_file')
    @patch('nwcsafpps_runner.l1c_processing.cpu_count')
    def test_create_l1c_processor_instance(self, cpu_count, config):
        """Test create the L1cProcessor instance."""
        cpu_count.return_value = 2
        config.return_value = self.config_complete

        with patch('nwcsafpps_runner.l1c_processing.ThreadPool') as mock:
            mock.return_value = None
            with tempfile.NamedTemporaryFile() as myconfig_file:
                l1c_proc = L1cProcessor(myconfig_file.name, 'seviri-l1c')

        mock.assert_called_once()

        self.assertEqual(l1c_proc.platform_name, 'unknown')
        self.assertEqual(l1c_proc.sensor, 'unknown')
        self.assertEqual(l1c_proc.orbit_number, 99999)
        self.assertEqual(l1c_proc.service, 'seviri-l1c')
        self.assertDictEqual(l1c_proc._l1c_processor_call_kwargs, {'engine': 'netcdf4', 'rotate': True})
        self.assertEqual(l1c_proc.result_home, '/san1/geo_in/lvl1c')
        self.assertEqual(l1c_proc.publish_topic, ['/1c/nc/0deg'])
        self.assertEqual(l1c_proc.subscribe_topics, ['/1b/hrit/0deg'])
        self.assertEqual(l1c_proc.message_data, None)
        self.assertEqual(l1c_proc.pool, None)
        self.assertEqual(l1c_proc.nameservers, None)

    @patch('nwcsafpps_runner.config.load_config_from_file')
    @patch('nwcsafpps_runner.l1c_processing.cpu_count')
    def test_create_l1c_processor_instance_minimal_config(self, cpu_count, config):
        """Test create the L1cProcessor instance, using a minimal configuration."""
        cpu_count.return_value = 1
        config.return_value = self.config_minimum

        with patch('nwcsafpps_runner.l1c_processing.ThreadPool') as mock:
            mock.return_value = None
            with tempfile.NamedTemporaryFile() as myconfig_file:
                l1c_proc = L1cProcessor(myconfig_file.name, 'seviri-l1c')

        mock.assert_called_once_with(1)

        self.assertEqual(l1c_proc.platform_name, 'unknown')
        self.assertEqual(l1c_proc.sensor, 'unknown')
        self.assertEqual(l1c_proc.orbit_number, 99999)
        self.assertEqual(l1c_proc.service, 'seviri-l1c')
        self.assertDictEqual(l1c_proc._l1c_processor_call_kwargs, {})
        self.assertEqual(l1c_proc.result_home, '/tmp')
        self.assertEqual(l1c_proc.publish_topic, ['/1c/nc/0deg'])
        self.assertEqual(l1c_proc.subscribe_topics, ['/1b/hrit/0deg'])
        self.assertEqual(l1c_proc.message_data, None)
        self.assertEqual(l1c_proc.pool, None)

    @patch('nwcsafpps_runner.config.load_config_from_file')
    @patch('nwcsafpps_runner.l1c_processing.cpu_count')
    def test_get_level1_files_from_dataset_viirs(self, cpu_count, config):
        """Test create the L1cProcessor instance, using a minimal configuration."""
        cpu_count.return_value = 1
        config.return_value = self.config_viirs_ok

        with patch('nwcsafpps_runner.l1c_processing.ThreadPool') as mock:
            mock.return_value = None
            with tempfile.NamedTemporaryFile() as myconfig_file:
                l1c_proc = L1cProcessor(myconfig_file.name, 'viirs-l1c')

        level1_dataset = TEST_VIIRS_MSG_DATA.get('dataset')

        l1c_proc.get_level1_files_from_dataset(level1_dataset)

        expected_filepath = ('/san1/polar_in/direct_readout/npp/lvl1/npp_20210601_0536_49711/' +
                             'GMODO_npp_d20210601_t0543111_e0544353_b49711_' +
                             'c20210601055246487163_cspp_dev.h5')
        self.assertEqual(l1c_proc.level1_files[0], expected_filepath)

    @patch('nwcsafpps_runner.config.load_config_from_file')
    @patch('nwcsafpps_runner.l1c_processing.cpu_count')
    def test_orbit_number_from_msg_viirs(self, cpu_count, config):
        """Test use orbit number from message."""
        cpu_count.return_value = 1
        config.return_value = self.config_viirs_orbit_number_from_msg_ok

        input_msg = Message.decode(rawstr=TEST_INPUT_MESSAGE_VIIRS_MSG)

        with patch('nwcsafpps_runner.l1c_processing.ThreadPool'):
            with tempfile.NamedTemporaryFile() as myconfig_file:
                l1c_proc = L1cProcessor(myconfig_file.name, 'viirs-l1c')
                l1c_proc.run(input_msg)

        expected_orbit_number = TEST_VIIRS_MSG_DATA.get('orbit_number')
        self.assertEqual(l1c_proc.orbit_number, expected_orbit_number)

    @patch('nwcsafpps_runner.config.load_config_from_file')
    @patch('nwcsafpps_runner.l1c_processing.cpu_count')
    def test_orbit_number_missing_in_msg_viirs(self, cpu_count, config):
        """Test use orbit number but missing in message."""
        cpu_count.return_value = 1
        config.return_value = self.config_viirs_orbit_number_from_msg_ok
        input_msg = Message.decode(rawstr=TEST_INPUT_MESSAGE_VIIRS_NO_ORBIT_MSG)

        with self.assertLogs('nwcsafpps_runner.l1c_processing', level='INFO') as cm:
            with patch('nwcsafpps_runner.l1c_processing.ThreadPool'):
                with tempfile.NamedTemporaryFile() as myconfig_file:
                    l1c_proc = L1cProcessor(myconfig_file.name, 'viirs-l1c')
                    l1c_proc.run(input_msg)
                expected_orbit_number = 99999
                self.assertEqual(l1c_proc.orbit_number, expected_orbit_number)

        self.assertEqual(cm.output[2], 'WARNING:nwcsafpps_runner.l1c_processing:You asked for orbit_number '
                                       'from the message, but its not there. Keep init orbit.')

    @patch('nwcsafpps_runner.config.load_config_from_file')
    @patch('nwcsafpps_runner.l1c_processing.cpu_count')
    def test_create_l1c_processor_instance_nameservers(self, cpu_count, config):
        """Test create the L1cProcessor instance."""
        cpu_count.return_value = 2
        config.return_value = self.config_complete_nameservers

        with patch('nwcsafpps_runner.l1c_processing.ThreadPool') as mock:
            mock.return_value = None
            with tempfile.NamedTemporaryFile() as myconfig_file:
                l1c_proc = L1cProcessor(myconfig_file.name, 'seviri-l1c')

        mock.assert_called_once()

        self.assertEqual(l1c_proc.platform_name, 'unknown')
        self.assertEqual(l1c_proc.sensor, 'unknown')
        self.assertEqual(l1c_proc.orbit_number, 99999)
        self.assertEqual(l1c_proc.service, 'seviri-l1c')
        self.assertDictEqual(l1c_proc._l1c_processor_call_kwargs, {'engine': 'netcdf4', 'rotate': True})
        self.assertEqual(l1c_proc.result_home, '/san1/geo_in/lvl1c')
        self.assertEqual(l1c_proc.publish_topic, ['/1c/nc/0deg'])
        self.assertEqual(l1c_proc.subscribe_topics, ['/1b/hrit/0deg'])
        self.assertEqual(l1c_proc.message_data, None)
        self.assertEqual(l1c_proc.pool, None)
        self.assertEqual(l1c_proc.nameservers, ['test.nameserver'])
