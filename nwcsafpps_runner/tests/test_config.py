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

"""Testing the config handling."""

import unittest
from unittest.mock import patch

import pytest

from nwcsafpps_runner.config import get_config

TEST_YAML_LVL1C_RUNNER_CONTENT_OK = """
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

TEST_YAML_PPS_RUNNER_CONFIG_OK = """
publish_topic: PPS
subscribe_topics: [/segment/SDR/1C] #[AAPP-HRPT,AAPP-PPS,EOS/1B,segment/SDR/1B,1c/nc/0deg]
sdr_processing: granules

pps_version: v2021
python: python
run_all_script:
  name: /path/to/pps/script/ppsRunAll.py
  flags:
    - 'no_cmaskprob'
    - 'no_cmic'

run_cmaprob_script: /path/to/pps/script/ppsCmaskProb.py
maximum_pps_processing_time_in_minutes: 20
run_cmask_prob: no
run_pps_cpp: no

log_rotation_days: 1
log_rotation_backup: 10

number_of_threads: 1
station: norrkoping

pps_outdir: /data/24/saf/polar_out/direct_readout
pps_statistics_dir: /data/24/saf/polar_out/monitoring/direct_readout

level1_dir: /path/to/lvl1c/data

nhsp_path: /path/to/nwp/data/atm_level_fiels/
nhsf_path: /path/to/nwp/data/surface_fields/
"""


@pytest.fixture
def fake_files(tmp_path):
    """Create directory with test files."""
    file_l1c = tmp_path / 'lvl1c_file.yaml'
    file_h = open(file_l1c, 'w')
    file_h.write(TEST_YAML_LVL1C_RUNNER_CONTENT_OK)
    file_h.close()

    file_pps = tmp_path / 'pps_file.yaml'
    file_h = open(file_pps, 'w')
    file_h.write(TEST_YAML_PPS_RUNNER_CONFIG_OK)
    file_h.close()
    return str(file_l1c), str(file_pps)


class TestGetConfig:
    """Test getting the yaml config from file."""

    def test_read_lvl1c_runner_config(self, fake_files):
        """Test loading and initialising the yaml config."""
        myconfig_filename, _ = fake_files

        result = get_config(myconfig_filename, service='seviri-l1c')

        expected = {'message_types': ['/1b/hrit/0deg'],
                    'publish_topic': ['/1c/nc/0deg'],
                    'instrument': 'seviri',
                    'num_of_cpus': 2,
                    'output_dir': '/san1/geo_in/lvl1c',
                    'l1cprocess_call_arguments': {'engine': 'netcdf4',
                                                  'rotate': True}}

        assert result == expected

    @patch('nwcsafpps_runner.config.socket.gethostname')
    def test_read_pps_runner_config(self, gethostname, fake_files):
        """Test loading and initialising the yaml config."""
        _, myconfig_filename = fake_files
        gethostname.return_value = "my.local.host"
        result = get_config(myconfig_filename, add_defaults=True)

        expected = {
                    'publish_topic': 'PPS',
                    'subscribe_topics': ['/segment/SDR/1C'],
                    'sdr_processing': 'granules',
                    'python': 'python',
                    'pps_version': 'v2021',
                    'run_all_script': {'name': '/path/to/pps/script/ppsRunAll.py',
                                       'flags': ['no_cmaskprob', 'no_cmic']},
                    'run_cmaprob_script': '/path/to/pps/script/ppsCmaskProb.py',
                    'maximum_pps_processing_time_in_minutes': 20,
                    'run_cmask_prob': False,
                    'run_pps_cpp': False,
                    'log_rotation_days': 1,
                    'log_rotation_backup': 10,
                    'number_of_threads': 1, 'station':
                    'norrkoping', 'pps_outdir':
                    '/data/24/saf/polar_out/direct_readout',
                    'pps_statistics_dir': '/data/24/saf/polar_out/monitoring/direct_readout',
                    'level1_dir': '/path/to/lvl1c/data',
                    'nhsp_path':
                    '/path/to/nwp/data/atm_level_fiels/',
                    'nhsf_path': '/path/to/nwp/data/surface_fields/',
                    'servername': 'my.local.host'}

        assert result == expected
