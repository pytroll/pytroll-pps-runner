#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2020, 2021 Pytroll developers

# Author(s):

#   Adam.Dybbroe <adam.dybbroe@smhi.se>

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

"""Testing the pps-hook code for the creation of a posttroll message.

The message is created from metadata partly read from a yaml config file.
"""

import unittest
from datetime import datetime
from multiprocessing import Manager
from unittest.mock import MagicMock, Mock, patch

import pytest
import yaml
import mock

from nwcsafpps_runner.pps_posttroll_hook import (PPSPublisher,
                                                 PPSMessage,
                                                 PostTrollMessage)

START_TIME1 = datetime.fromisoformat("2020-12-17T14:08:25.800000")
END_TIME1 = datetime.fromisoformat("2020-12-17T14:09:50")

START_TIME2 = datetime.fromisoformat("2020-12-17T13:25:45.900000")
END_TIME2 = datetime.fromisoformat("2020-12-17T13:27:08.700000")


# Test yaml content:
# TEST_YAML_CONTENT_OK = """
# pps_hook:
#     post_hook: !!python/object:nwcsafpps_runner.pps_posttroll_hook.PPSMessage
#       description: "This is a pps post hook for PostTroll messaging"
#       metadata:
#         posttroll_topic: "PPSv2018"
#         station: "norrkoping"
#         output_format: "CF"
#         level: "2"
#         variant: DR
# """

TEST_YAML_CONTENT_OK = """
pps_hook:
    post_hook: !!python/object:nwcsafpps_runner.pps_posttroll_hook.PPSMessage
      description: "This is a pps post hook for PostTroll messaging"
      metadata:
        station: "norrkoping"
        output_format: "CF"
        level: "2"
        variant: DR
        geo_or_polar: "polar"
        software: "NWCSAF-PPSv2018"
"""

TEST_YAML_CONTENT_INSUFFICIENT = """
pps_hook:
    post_hook: !!python/object:nwcsafpps_runner.pps_posttroll_hook.PPSMessage
      description: "This is a pps post hook for PostTroll messaging"
      metadata:
        station: "norrkoping"
        variant: DR
"""

TEST_YAML_CONTENT_SPECIFY_PUBLISH_TOPIC_OK = """
pps_hook:
    post_hook: !!python/object:nwcsafpps_runner.pps_posttroll_hook.PPSMessage
      description: "This is a pps post hook for PostTroll messaging"
      metadata:
        posttroll_topic: "/PPSv2018"
"""

TEST_YAML_NAMESERVERS_CONTENT_OK = """
pps_hook:
    post_hook: !!python/object:nwcsafpps_runner.pps_posttroll_hook.PPSMessage
      description: "This is a pps post hook for PostTroll messaging"
      metadata:
        station: "norrkoping"
        output_format: "CF"
        level: "2"
        variant: DR
        geo_or_polar: "polar"
        software: "NWCSAF-PPSv2018"
        nameservers:
        - 'test1'
        - 'test2'
"""

TEST_YAML_NAMESERVERS_IS_LIST = """
pps_hook:
    post_hook: !!python/object:nwcsafpps_runner.pps_posttroll_hook.PPSMessage
      description: "This is a pps post hook for PostTroll messaging"
      metadata:
        station: "norrkoping"
        output_format: "CF"
        level: "2"
        variant: DR
        geo_or_polar: "polar"
        software: "NWCSAF-PPSv2018"
        nameservers: test1
"""


def create_instance_from_yaml(yaml_content_str):
    """Create a PPSMessage instance from a yaml file."""
    # from nwcsafpps_runner.pps_posttroll_hook import PPSMessage
    return yaml.load(yaml_content_str, Loader=yaml.UnsafeLoader)


class TestPPSPublisher(unittest.TestCase):
    """Test the PPSPublisher."""

    def setUp(self):
        self.test_nameservers = ['test1', 'test2']

    @mock.patch("nwcsafpps_runner.pps_posttroll_hook.time.sleep")
    def test_called_with_nameservers(self, mock_sleeep):
        """Test calling the PPSPublisher with a list of specified nameservers."""
        mymock = MagicMock()

        manager = Manager()
        publisher_q = manager.Queue()

        with patch('nwcsafpps_runner.pps_posttroll_hook.Publish', return_value=mymock) as mypatch:
            pub_thread = PPSPublisher(publisher_q, self.test_nameservers)
            pub_thread.start()

        mypatch.assert_called_with('PPS', 0, nameservers=self.test_nameservers)
        pub_thread.stop()

    @mock.patch("nwcsafpps_runner.pps_posttroll_hook.time.sleep")
    def test_called_without_nameservers(self, mock_sleeep):
        """Test calling the PPSPublisher without specifying any nameservers."""
        mymock = MagicMock()

        manager = Manager()
        publisher_q = manager.Queue()
        publisher_q.put("dummy_posttroll_message")
        publisher_q.put(None)
        with patch('nwcsafpps_runner.pps_posttroll_hook.Publish', return_value=mymock) as mypatch:
            pub_thread = PPSPublisher(publisher_q)
            pub_thread.start()

        mypatch.assert_called_with('PPS', 0, nameservers=None)
        pub_thread.join(timeout=0.001)
        pub_thread.stop()


class TestPPSMessage(unittest.TestCase):
    """Test the functionality of the PPSMessage object."""

    def setUp(self):
        self.pps_message_instance_from_yaml_config = create_instance_from_yaml(TEST_YAML_CONTENT_OK)

    def test_class_instance_can_be_called(self):
        """Test that the PPSMessage can be instantiated and called."""
        mymock = MagicMock()
        mymock.send = Mock(return_value=None)

        test_mda = {'filename': 'xxx', 'start_time': None, 'end_time': None, 'sensor': 'viirs'}
        with patch('nwcsafpps_runner.pps_posttroll_hook.PostTrollMessage', return_value=mymock) as mypatch:
            _ = self.pps_message_instance_from_yaml_config['pps_hook']['post_hook'](0, test_mda)
        mypatch.assert_called_once()


class TestPostTrollMessage(unittest.TestCase):
    """Test the functionality of the PostTrollMessage object."""

    def setUp(self):
        self.pps_message_instance_from_yaml_config_ok = create_instance_from_yaml(TEST_YAML_CONTENT_OK)
        self.pps_message_instance_from_yaml_config_fail = create_instance_from_yaml(TEST_YAML_CONTENT_INSUFFICIENT)
        self.pps_message_instance_from_yaml_config_ok_publish_topic = create_instance_from_yaml(
            TEST_YAML_CONTENT_SPECIFY_PUBLISH_TOPIC_OK)
        self.pps_message_instance_from_yaml_nameservers_config_ok = create_instance_from_yaml(
            TEST_YAML_NAMESERVERS_CONTENT_OK)
        self.pps_message_instance_from_yaml_nameservers_is_list = create_instance_from_yaml(
            TEST_YAML_NAMESERVERS_IS_LIST)

        self.metadata = {'module': "ppsCmask",
                         'station': 'norrkoping',
                         'output_format': 'CF',
                         'geo_or_polar': 'polar',
                         'software': 'NWCSAF-PPSv2018',
                         'level': '2', 'variant': 'DR'}
        self.metadata_with_filename = {'module': "ppsCmask",
                                       'station': 'norrkoping',
                                       'output_format': 'CF',
                                       'geo_or_polar': 'polar',
                                       'software': 'NWCSAF-PPSv2018',
                                       'level': '2', 'variant': 'DR', 'filename': '/my_dummy_dir/xxx'}
        self.metadata_with_start_and_end_times = {'module': "ppsCmask",
                                                  'station': 'norrkoping', 'output_format': 'CF',
                                                  'level': '2', 'variant': 'DR',
                                                  'geo_or_polar': 'polar',
                                                  'software': 'NWCSAF-PPSv2018',
                                                  'start_time': None, 'end_time': None}
        self.metadata_with_platform_name = {'module': "ppsCmask",
                                            'station': 'norrkoping', 'output_format': 'CF',
                                            'level': '2', 'variant': 'DR',
                                            'geo_or_polar': 'polar',
                                            'software': 'NWCSAF-PPSv2018',
                                            'platform_name': 'npp'}

        self.mandatory_fields = {"output_format": "format"}

    def test_class_instance_missing_filename(self):
        """Test that the PPSMessage can be instantiated and called."""
        test_mda = {'filename': "", 'start_time': None, 'end_time': None}
        test_filename = PostTrollMessage(0, test_mda)
        test_filename.send()
        test_status = PostTrollMessage(1, test_mda)
        test_status.send()

    @patch('nwcsafpps_runner.pps_posttroll_hook.PostTrollMessage.check_metadata_contains_filename')
    @patch('nwcsafpps_runner.pps_posttroll_hook.PostTrollMessage.check_metadata_contains_mandatory_parameters')
    def test_send_method(self, mandatory_param, filename):
        """Test that the message contains the mandatory fields."""
        from nwcsafpps_runner.pps_posttroll_hook import PostTrollMessage

        mandatory_param.return_value = True
        filename.return_value = True
        my_metadata = self.metadata.copy()
        my_metadata["filename"] = "dummy"

        with patch.object(PostTrollMessage, 'publish_message', return_value=None) as mock_method_publish:
            with patch.object(PostTrollMessage, 'create_message', return_value=None) as mock_method_create:
                posttroll_message = PostTrollMessage(0, my_metadata)
                posttroll_message.send()
                self.assertEqual(mock_method_publish.call_count, 1)
                self.assertEqual(mock_method_create.call_count, 1)

    @patch('nwcsafpps_runner.pps_posttroll_hook.PostTrollMessage.check_metadata_contains_mandatory_parameters')
    def test_check_metadata_contains_filename(self, mandatory_param):
        """Test that the filename has to be included in the metadata."""
        from nwcsafpps_runner.pps_posttroll_hook import PostTrollMessage

        mandatory_param.return_value = True

        with pytest.raises(KeyError) as exec_info:
            posttroll_message = PostTrollMessage(0, self.metadata)

        exception_raised = exec_info.value
        self.assertEqual(str(exception_raised), "'filename'")

        posttroll_message = PostTrollMessage(0, self.metadata_with_filename)
        self.assertIsInstance(posttroll_message, PostTrollMessage)

    def test_check_metadata_contains_mandatory_parameters_fails(self):
        """Test that the filename has to be included in the metadata."""
        from nwcsafpps_runner.pps_posttroll_hook import PostTrollMessage

        with pytest.raises(AttributeError) as exec_info:
            posttroll_message = PostTrollMessage(0, self.metadata)
        exception_raised = exec_info.value
        self.assertEqual(str(exception_raised), "start_time is a required attribute but is missing in metadata!")

    @patch('socket.gethostname')
    def test_create_message_notopic_metadata_nosegment(self, socket_gethostname):
        """Test creating a message with header/topic, type and content."""
        from nwcsafpps_runner.pps_posttroll_hook import PostTrollMessage

        socket_gethostname.return_value = 'TEST_SERVERNAME'

        metadata = {'module': "ppsCmask",
                    'output_format': 'CF',
                    'level': '2',
                    'variant': 'DR',
                    'geo_or_polar': 'polar',
                    'software': 'NWCSAF-PPSv2018',
                    'start_time': START_TIME1, 'end_time': END_TIME1,
                    'sensor': 'viirs',
                    'filename': ['/my_dummy_dir/xxx', '/my_dummy_dir/yyy'],
                    'platform_name': 'npp'}

        posttroll_message = PostTrollMessage(0, metadata)
        result_message = posttroll_message.create_message('OK')
        expected_message_header = "/polar/direct_readout/CF/2/pps/CMA/NWCSAF-PPSv2018/"
        self.assertEqual('dataset', result_message['type'])
        self.assertEqual(expected_message_header, result_message['header'])

    @patch('socket.gethostname')
    def test_create_message_with_topic(self, socket_gethostname):
        """Test creating a message with header/topic, type and content."""
        from nwcsafpps_runner.pps_posttroll_hook import PostTrollMessage

        socket_gethostname.return_value = 'TEST_SERVERNAME'

        metadata = {'publish_topic': '/my/pps/publish/topic/{pps_product}/',
                    'output_format': 'CF',
                    'level': '2',
                    'variant': 'DR',
                    'geo_or_polar': 'polar',
                    'software': 'NWCSAF-PPSv2018',
                    'start_time': START_TIME1, 'end_time': END_TIME1,
                    'sensor': 'viirs',
                    'filename': '/my_dummy_dir/xxx',
                    'platform_name': 'npp'}

        posttroll_message = PostTrollMessage(0, metadata)

        result_message = posttroll_message.create_message('OK')

        expected_message_header = "/my/pps/publish/topic/UNKNOWN/"
        self.assertEqual(expected_message_header, result_message['header'])

    @patch('socket.gethostname')
    def test_create_message_without_filename(self, socket_gethostname):
        """Test creating a message with header/topic, type and content."""
        from nwcsafpps_runner.pps_posttroll_hook import PostTrollMessage

        socket_gethostname.return_value = 'TEST_SERVERNAME'

        metadata = {'publish_topic': '/my/pps/publish/topic/{pps_product}/',
                    'output_format': 'CF',
                    'level': '2',
                    'variant': 'DR',
                    'geo_or_polar': 'polar',
                    'software': 'NWCSAF-PPSv2018',
                    'start_time': START_TIME1, 'end_time': END_TIME1,
                    'sensor': 'viirs',
                    'filename': '',
                    'platform_name': 'npp'}

        posttroll_message = PostTrollMessage(0, metadata)
        result_message = posttroll_message.create_message('OK')
        self.assertFalse("uri" in result_message["content"])

    @patch('socket.gethostname')
    def test_create_message_with_topic_pattern(self, socket_gethostname):
        """Test creating a message with header/topic that is a pattern, type and content."""
        from nwcsafpps_runner.pps_posttroll_hook import PostTrollMessage

        socket_gethostname.return_value = 'TEST_SERVERNAME'

        metadata = {'publish_topic': '/my/pps/publish/topic/{sensor}/{pps_product}/',
                    'output_format': 'CF',
                    'level': '2',
                    'variant': 'DR',
                    'geo_or_polar': 'polar',
                    'software': 'NWCSAF-PPSv2018',
                    'start_time': START_TIME1, 'end_time': END_TIME1,
                    'sensor': 'viirs',
                    'filename': '/my_dummy_dir/xxx',
                    'platform_name': 'npp'}

        posttroll_message = PostTrollMessage(0, metadata)

        result_message = posttroll_message.create_message('OK')

        expected_message_header = "/my/pps/publish/topic/viirs/UNKNOWN/"
        self.assertEqual(expected_message_header, result_message['header'])

    @patch('nwcsafpps_runner.pps_posttroll_hook.PostTrollMessage.check_metadata_contains_filename')
    @patch('nwcsafpps_runner.pps_posttroll_hook.PostTrollMessage.check_metadata_contains_mandatory_parameters')
    @patch('socket.gethostname')
    def test_get_message_with_uri_and_uid(self, socket_gethostname, mandatory_param, filename):
        """Test that the filename has to be included in the metadata."""
        from nwcsafpps_runner.pps_posttroll_hook import PostTrollMessage

        socket_gethostname.return_value = 'TEST_SERVERNAME'
        mandatory_param.return_value = True
        filename.return_value = True

        metadata = self.metadata_with_start_and_end_times
        metadata['start_time'] = START_TIME1
        metadata['end_time'] = END_TIME1

        posttroll_message = PostTrollMessage(0, metadata)
        mymessage = posttroll_message.get_message_with_uri_and_uid()

        self.assertFalse(mymessage)

        metadata.update({'filename': '/my_dummy_dir/xxx'})
        result_message = {'uri': '/my_dummy_dir/xxx', 'uid': 'xxx'}

        posttroll_message = PostTrollMessage(0, metadata)
        mymessage = posttroll_message.get_message_with_uri_and_uid()
        self.assertDictEqual(mymessage, result_message)

        metadata.update({'filename': ['/my_dummy_dir/xxx', '/my_dummy_dir/xxx2']})
        result_message = {'dataset': [{'uri': '/my_dummy_dir/xxx', 'uid': 'xxx'},
                                      {'uri': '/my_dummy_dir/xxx2', 'uid': 'xxx2'}]}

        posttroll_message = PostTrollMessage(0, metadata)
        mymessage = posttroll_message.get_message_with_uri_and_uid()
        self.assertDictEqual(mymessage, result_message)

    @patch('nwcsafpps_runner.pps_posttroll_hook.PostTrollMessage.check_metadata_contains_filename')
    @patch('nwcsafpps_runner.pps_posttroll_hook.PostTrollMessage.check_metadata_contains_mandatory_parameters')
    def test_create_message_content_from_metadata(self, mandatory_param, filename):
        """Test the creation of the message content from the inout metadata."""
        from nwcsafpps_runner.pps_posttroll_hook import PostTrollMessage

        mandatory_param.return_value = True
        filename.return_value = True

        metadata = self.metadata_with_platform_name
        posttroll_message = PostTrollMessage(0, metadata)
        msg_content = posttroll_message.create_message_content_from_metadata()
        self.assertIn('platform_name', msg_content)
        self.assertEqual(msg_content['platform_name'], 'Suomi-NPP')

        metadata.update({'platform_name': 'noaa20'})
        posttroll_message = PostTrollMessage(0, metadata)
        msg_content = posttroll_message.create_message_content_from_metadata()
        self.assertEqual(msg_content['platform_name'], 'NOAA-20')

        metadata.update({'platform_name': 'NOAA-20'})
        posttroll_message = PostTrollMessage(0, metadata)
        msg_content = posttroll_message.create_message_content_from_metadata()
        self.assertEqual(msg_content['platform_name'], 'NOAA-20')

    @patch('nwcsafpps_runner.pps_posttroll_hook.PostTrollMessage.check_metadata_contains_filename')
    @patch('nwcsafpps_runner.pps_posttroll_hook.PostTrollMessage.check_metadata_contains_mandatory_parameters')
    def test_check_nameservers_as_metadata(self, mandatory_param, filename):
        """Test if nameservers as metadata is used."""
        from nwcsafpps_runner.pps_posttroll_hook import PostTrollMessage

        mandatory_param.return_value = True
        filename.return_value = True
        metadata = self.pps_message_instance_from_yaml_nameservers_config_ok['pps_hook']['post_hook'].metadata
        posttroll_message = PostTrollMessage(0, metadata)

        result = posttroll_message.get_nameservers()
        self.assertEqual(result, ['test1', 'test2'])

    @patch('nwcsafpps_runner.pps_posttroll_hook.PostTrollMessage.check_metadata_contains_filename')
    @patch('nwcsafpps_runner.pps_posttroll_hook.PostTrollMessage.check_metadata_contains_mandatory_parameters')
    def test_metadata_no_nameservers(self, mandatory_param, filename):
        """Test that the no nameservers in metadata returns None."""
        from nwcsafpps_runner.pps_posttroll_hook import PostTrollMessage

        mandatory_param.return_value = True
        filename.return_value = True
        metadata = self.pps_message_instance_from_yaml_config_ok['pps_hook']['post_hook'].metadata
        posttroll_message = PostTrollMessage(0, metadata)

        result = posttroll_message.get_nameservers()
        self.assertEqual(result, None)

    @patch('nwcsafpps_runner.pps_posttroll_hook.PostTrollMessage.check_metadata_contains_filename')
    @patch('nwcsafpps_runner.pps_posttroll_hook.PostTrollMessage.check_metadata_contains_mandatory_parameters')
    def test_metadata_nameservers_is_list(self, mandatory_param, filename):
        """Test that the nameservers in metadata is list."""
        from nwcsafpps_runner.pps_posttroll_hook import PostTrollMessage

        mandatory_param.return_value = True
        filename.return_value = True
        metadata = self.pps_message_instance_from_yaml_nameservers_is_list['pps_hook']['post_hook'].metadata
        posttroll_message = PostTrollMessage(0, metadata)

        result = posttroll_message.get_nameservers()
        self.assertEqual(result, None)

    @mock.patch("nwcsafpps_runner.pps_posttroll_hook.time.sleep")
    @mock.patch("nwcsafpps_runner.pps_posttroll_hook.Publish")
    def test_publish(self, mock_publish, mock_sleep):
        """Test that the message contains the mandatory fields."""
        from nwcsafpps_runner.pps_posttroll_hook import PostTrollMessage
        my_metadata = self.metadata_with_start_and_end_times.copy()
        my_metadata["filename"] = "dummy"

        posttroll_message = PostTrollMessage(0, my_metadata)
        posttroll_message.send()
