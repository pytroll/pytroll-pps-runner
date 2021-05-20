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
from unittest.mock import patch, Mock, MagicMock
import unittest
from posttroll.message import Message
from datetime import datetime

from nwcsafpps_runner.message_utils import publish_l1c, prepare_l1c_message
from nwcsafpps_runner.l1c_processing import check_message_okay
from nwcsafpps_runner.l1c_processing import check_service_name_okay
from nwcsafpps_runner.l1c_processing import L1cProcessor
from nwcsafpps_runner.l1c_processing import ServiceNameNotSupported


class MyFakePublisher(object):

    def __init__(self):
        pass

    def send(self, message):
        pass


#TEST_INPUT_MSG = """pytroll://1b/hrit/0deg dataset safusr.u@lxserv1043.smhi.se 2021-05-18T05:58:49.576064 v1.01 application/json {"data_type": "MSG4", "orig_platform_name": "MSG4", "start_time": "2021-05-18T05:45:00", "variant": "0DEG", "series": "MSG4", "platform_name": "Meteosat-11", "channel": "", "nominal_time": "2021-05-18T05:45:00", "compressed": "", "origin": "172.18.0.248:9093", "dataset": [{"uri": "/tmp/myfile", "uid": "myfile"}, {"uri": "/tmp/myfile", "uid": "myfile"}], "sensor": ["seviri"]}"""

TEST_INPUT_MSG = """pytroll://1b/hrit/0deg dataset safusr.u@lxserv1043.smhi.se 2021-05-18T14:28:54.154172 v1.01 application/json {"data_type": "MSG4", "orig_platform_name": "MSG4", "start_time": "2021-05-18T14:15:00", "variant": "0DEG", "series": "MSG4", "platform_name": "Meteosat-11", "channel": "", "nominal_time": "2021-05-18T14:15:00", "compressed": "", "origin": "172.18.0.248:9093", "dataset": [{"uri": "/san1/geo_in/0deg/H-000-MSG4__-MSG4________-_________-PRO______-202105181415-__", "uid": "H-000-MSG4__-MSG4________-_________-PRO______-202105181415-__"}, {"uri": "/san1/geo_in/0deg/H-000-MSG4__-MSG4________-HRV______-000001___-202105181415-__", "uid": "H-000-MSG4__-MSG4________-HRV______-000001___-202105181415-__"}], "sensor": ["seviri"]}"""

TEST_INPUT_MSG_NO_DATASET = """pytroll://1b/hrit/0deg file safusr.u@lxserv1043.smhi.se 2021-05-18T14:28:54.154172 v1.01 application/json {"data_type": "MSG4", "orig_platform_name": "MSG4", "start_time": "2021-05-18T14:15:00", "variant": "0DEG", "series": "MSG4", "platform_name": "Meteosat-11", "channel": "", "nominal_time": "2021-05-18T14:15:00", "compressed": "", "origin": "172.18.0.248:9093", "file": "/san1/geo_in/0deg/H-000-MSG4__-MSG4________-_________-PRO______-202105181415-__", "sensor": ["seviri"]}"""


TEST_INPUT_MSG_NO_PLATFORM_NAME = """pytroll://1b/hrit/0deg dataset safusr.u@lxserv1043.smhi.se 2021-05-18T14:28:54.154172 v1.01 application/json {"data_type": "MSG4", "orig_platform_name": "MSG4", "start_time": "2021-05-18T14:15:00", "variant": "0DEG", "series": "MSG4", "channel": "", "nominal_time": "2021-05-18T14:15:00", "compressed": "", "origin": "172.18.0.248:9093", "dataset": [{"uri": "/san1/geo_in/0deg/H-000-MSG4__-MSG4________-_________-PRO______-202105181415-__", "uid": "H-000-MSG4__-MSG4________-_________-PRO______-202105181415-__"}, {"uri": "/san1/geo_in/0deg/H-000-MSG4__-MSG4________-HRV______-000001___-202105181415-__", "uid": "H-000-MSG4__-MSG4________-HRV______-000001___-202105181415-__"}], "sensor": ["seviri"]}"""


class TestPublishMessage(unittest.TestCase):

    @patch('nwcsafpps_runner.message_utils.socket.gethostname')
    def test_create_publish_message(self, gethostname):
        """Test the creation of the publish message."""

        gethostname.return_value = "my_local_server"

        my_fake_publisher = MyFakePublisher()

        my_fake_level1c_file = '/my/level1c/file/path/level1c.nc'

        input_msg = Message.decode(rawstr=TEST_INPUT_MSG)

        result = prepare_l1c_message(my_fake_level1c_file, input_msg.data, orbit=99999)

        #publish_l1c(my_fake_publisher, to_send, ['/1c/nc/0deg'])

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

    def test_check_service_name_okay(self):

        check_service_name_okay('seviri-l1c')
        check_service_name_okay('viirs-l1c')
        check_service_name_okay('avhrr-l1c')
        check_service_name_okay('modis-l1c')

        self.assertRaises(ServiceNameNotSupported, check_service_name_okay, 'seviri')

        with pytest.raises(ServiceNameNotSupported) as exec_info:
            check_service_name_okay('avhrr')

        exception_raised = exec_info.value
        self.assertTrue('Service name avhrr is not yet supported' == str(exception_raised))

    def test_check_message_okay_message_ok(self):

        input_msg = Message.decode(rawstr=TEST_INPUT_MSG)

        result = check_message_okay(input_msg)
        self.assertTrue(result)

    def test_check_message_okay_message_has_no_dataset(self):
        """Test that message is not okay if it is not a dataset."""

        input_msg = Message.decode(rawstr=TEST_INPUT_MSG_NO_DATASET)
        result = check_message_okay(input_msg)
        self.assertTrue(result is False)

    def test_check_message_okay_message_has_no_platform_name(self):
        """Test that message is not okay if it is not a dataset."""

        input_msg = Message.decode(rawstr=TEST_INPUT_MSG_NO_PLATFORM_NAME)
        result = check_message_okay(input_msg)
        self.assertTrue(result is False)
