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

from nwcsafpps_runner.config import get_config_from_yamlfile
from nwcsafpps_runner.config import get_config_yaml


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
nhsp_prefix: LL02_NHSP_
nhsf_prefix: LL02_NHSF_
ecmwf_prefix: LL02_NHSF

nhsf_file_name_sift: '{ecmwf_prefix:9s}_{analysis_time:%Y%m%d%H%M}+{forecast_step:d}H00M'

nwp_static_surface: /san1/pps/import/NWP_data/lsm_z.grib1
nwp_output_prefix: LL02_NHSPSF_
nwp_outdir: /san1/pps/import/NWP_data/source
pps_nwp_requirements: /san1/pps/import/NWP_data/pps_nwp_list_of_required_fields.txt

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


def create_config_from_yaml(yaml_content_str):
    """Create aapp-runner config dict from a yaml file."""
    return yaml.load(yaml_content_str, Loader=yaml.FullLoader)


class TestGetConfig(unittest.TestCase):
    """Test getting the yaml config from file"""

    def setUp(self):
        self.config_lvl1c_complete = create_config_from_yaml(TEST_YAML_LVL1C_RUNNER_CONTENT_OK)
        self.config_pps_complete = create_config_from_yaml(TEST_YAML_PPS_RUNNER_CONFIG_OK)

    @patch('nwcsafpps_runner.config.load_config_from_file')
    def test_read_lvl1c_runner_config(self, config):
        """Test loading and initialising the yaml config"""
        config.return_value = self.config_lvl1c_complete
        myconfig_filename = '/tmp/my/config/file'

        result = get_config_from_yamlfile(myconfig_filename, 'seviri-l1c')

        expected = {'message_types': ['/1b/hrit/0deg'],
                    'publish_topic': ['/1c/nc/0deg'],
                    'instrument': 'seviri',
                    'num_of_cpus': 2,
                    'output_dir': '/san1/geo_in/lvl1c',
                    'l1cprocess_call_arguments': {'engine': 'netcdf4',
                                                  'rotate': True}}

        self.assertDictEqual(result, expected)

    @patch('nwcsafpps_runner.config.load_config_from_file')
    @patch('nwcsafpps_runner.config.socket.gethostname')
    def test_read_pps_runner_config(self, gethostname, config):
        """Test loading and initialising the yaml config"""
        gethostname.return_value = "my.local.host"
        config.return_value = self.config_pps_complete
        myconfig_filename = '/tmp/my/config/file'

        result = get_config_yaml(myconfig_filename)

        expected = {'nhsp_prefix': 'LL02_NHSP_',
                    'nhsf_prefix': 'LL02_NHSF_',
                    'ecmwf_prefix': 'LL02_NHSF',
                    'nhsf_file_name_sift': ('{ecmwf_prefix:9s}_' +
                                            '{analysis_time:%Y%m%d%H%M}+' +
                                            '{forecast_step:d}H00M'),
                    'nwp_static_surface': '/san1/pps/import/NWP_data/lsm_z.grib1',
                    'nwp_output_prefix': 'LL02_NHSPSF_',
                    'nwp_outdir': '/san1/pps/import/NWP_data/source',
                    'pps_nwp_requirements': '/san1/pps/import/NWP_data/pps_nwp_list_of_required_fields.txt',
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

        self.assertDictEqual(result, expected)
