#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2022 Pytroll Community

# Author(s):

#   Adam.Dybbroe <a000680@c21856.ad.smhi.se>
#   Nina.Hakansson

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

"""Test the nwp_prepare runner code."""
import unittest

import pytest
from posttroll.message import Message
# from posttroll.testing import patched_subscriber_recv
# from nwcsafpps_runner.pps_collector_lib import pps_collector_runner
from nwcsafpps_runner.message_utils import prepare_pps_collector_message
from nwcsafpps_runner.config import get_config

TEST_INPUT_MSG = (
    """pytroll://collection/SDR+CF/1+2/CloudProducts/ collection auser@some.server.se """ +
    """2023-05-15T04:30:21.034050 v1.01 application/json """ +
    """{"start_time": "2023-05-15T04:02:52.300000",""" +
    """ "end_time": "2023-05-15T04:15:38.900000",""" +
    """ "orbit_number": 2637,""" +
    """ "platform_name": "NOAA-21",""" +
    """ "format": "SDR",""" +
    """ "type": "HDF5",""" +
    """ "data_processing_level": "1B",""" +
    """ "variant": "DR",""" +
    """ "orig_orbit_number": 2636,""" +
    """ "sensor": ["viirs"],""" +
    """ "collection_area_id": "euron1",""" +
    """ "collection": [ """ +
    """{"dataset": [""" +
    """ {"uri": "/my_dir/GMODO_j02_d20230515_t0402523_e0404152_b02637_c20230515040842931901_cspp_dev.h5",""" +
    """ "uid": "GMODO_j02_d20230515_t0402523_e0404152_b02637_c20230515040842931901_cspp_dev.h5"},""" +
    """ {"uri": "/my_dir/GMTCO_j02_d20230515_t0402523_e0404152_b02637_c20230515040842847426_cspp_dev.h5",""" +
    """ "uid": "GMTCO_j02_d20230515_t0402523_e0404152_b02637_c20230515040842847426_cspp_dev.h5"},""" +
    """ {"uri": "/my_dir/SVM01_j02_d20230515_t0402523_e0404152_b02637_c20230515040918683116_cspp_dev.h5",""" +
    """ "uid": "SVM01_j02_d20230515_t0402523_e0404152_b02637_c20230515040918683116_cspp_dev.h5"},""" +
    """ {"uri": "/my_dir/SVM02_j02_d20230515_t0402523_e0404152_b02637_c20230515040918729002_cspp_dev.h5",""" +
    """ "uid": "SVM02_j02_d20230515_t0402523_e0404152_b02637_c20230515040918729002_cspp_dev.h5"},""" +
    """ {"uri": "/my_dir/SVM03_j02_d20230515_t0402523_e0404152_b02637_c20230515040918778479_cspp_dev.h5",""" +
    """ "uid": "SVM03_j02_d20230515_t0402523_e0404152_b02637_c20230515040918778479_cspp_dev.h5"},""" +
    """ {"uri": "/my_dir/SVM04_j02_d20230515_t0402523_e0404152_b02637_c20230515040918824679_cspp_dev.h5",""" +
    """ "uid": "SVM04_j02_d20230515_t0402523_e0404152_b02637_c20230515040918824679_cspp_dev.h5"},""" +
    """ {"uri": "/my_dir/SVM05_j02_d20230515_t0402523_e0404152_b02637_c20230515040918871757_cspp_dev.h5",""" +
    """ "uid": "SVM05_j02_d20230515_t0402523_e0404152_b02637_c20230515040918871757_cspp_dev.h5"},""" +
    """ {"uri": "/my_dir/SVM06_j02_d20230515_t0402523_e0404152_b02637_c20230515040918926725_cspp_dev.h5",""" +
    """ "uid": "SVM06_j02_d20230515_t0402523_e0404152_b02637_c20230515040918926725_cspp_dev.h5"},""" +
    """ {"uri": "/my_dir/SVM07_j02_d20230515_t0402523_e0404152_b02637_c20230515040918982899_cspp_dev.h5",""" +
    """ "uid": "SVM07_j02_d20230515_t0402523_e0404152_b02637_c20230515040918982899_cspp_dev.h5"},""" +
    """ {"uri": "/my_dir/SVM08_j02_d20230515_t0402523_e0404152_b02637_c20230515040919028526_cspp_dev.h5",""" +
    """ "uid": "SVM08_j02_d20230515_t0402523_e0404152_b02637_c20230515040919028526_cspp_dev.h5"},""" +
    """ {"uri": "/my_dir/SVM09_j02_d20230515_t0402523_e0404152_b02637_c20230515040919069935_cspp_dev.h5",""" +
    """ "uid": "SVM09_j02_d20230515_t0402523_e0404152_b02637_c20230515040919069935_cspp_dev.h5"},""" +
    """ {"uri": "/my_dir/SVM10_j02_d20230515_t0402523_e0404152_b02637_c20230515040919110030_cspp_dev.h5",""" +
    """ "uid": "SVM10_j02_d20230515_t0402523_e0404152_b02637_c20230515040919110030_cspp_dev.h5"},""" +
    """ {"uri": "/my_dir/SVM11_j02_d20230515_t0402523_e0404152_b02637_c20230515040919155907_cspp_dev.h5",""" +
    """ "uid": "SVM11_j02_d20230515_t0402523_e0404152_b02637_c20230515040919155907_cspp_dev.h5"},""" +
    """ {"uri": "/my_dir/SVM12_j02_d20230515_t0402523_e0404152_b02637_c20230515040919206051_cspp_dev.h5",""" +
    """ "uid": "SVM12_j02_d20230515_t0402523_e0404152_b02637_c20230515040919206051_cspp_dev.h5"},""" +
    """ {"uri": "/my_dir/SVM13_j02_d20230515_t0402523_e0404152_b02637_c20230515040919232307_cspp_dev.h5",""" +
    """ "uid": "SVM13_j02_d20230515_t0402523_e0404152_b02637_c20230515040919232307_cspp_dev.h5"},""" +
    """ {"uri": "/my_dir/SVM14_j02_d20230515_t0402523_e0404152_b02637_c20230515040919281872_cspp_dev.h5",""" +
    """ "uid": "SVM14_j02_d20230515_t0402523_e0404152_b02637_c20230515040919281872_cspp_dev.h5"},""" +
    """ {"uri": "/my_dir/SVM15_j02_d20230515_t0402523_e0404152_b02637_c20230515040919325359_cspp_dev.h5",""" +
    """ "uid": "SVM15_j02_d20230515_t0402523_e0404152_b02637_c20230515040919325359_cspp_dev.h5"},""" +
    """ {"uri": "/my_dir/SVM16_j02_d20230515_t0402523_e0404152_b02637_c20230515040919379332_cspp_dev.h5",""" +
    """ "uid": "SVM16_j02_d20230515_t0402523_e0404152_b02637_c20230515040919379332_cspp_dev.h5"},""" +
    """ {"uri": "/my_dir/GIMGO_j02_d20230515_t0402523_e0404152_b02637_c20230515040842366314_cspp_dev.h5",""" +
    """ "uid": "GIMGO_j02_d20230515_t0402523_e0404152_b02637_c20230515040842366314_cspp_dev.h5"},""" +
    """ {"uri": "/my_dir/GITCO_j02_d20230515_t0402523_e0404152_b02637_c20230515040842104190_cspp_dev.h5",""" +
    """ "uid": "GITCO_j02_d20230515_t0402523_e0404152_b02637_c20230515040842104190_cspp_dev.h5"},""" +
    """ {"uri": "/my_dir/SVI01_j02_d20230515_t0402523_e0404152_b02637_c20230515040918210080_cspp_dev.h5",""" +
    """ "uid": "SVI01_j02_d20230515_t0402523_e0404152_b02637_c20230515040918210080_cspp_dev.h5"},""" +
    """ {"uri": "/my_dir/SVI02_j02_d20230515_t0402523_e0404152_b02637_c20230515040918311250_cspp_dev.h5",""" +
    """ "uid": "SVI02_j02_d20230515_t0402523_e0404152_b02637_c20230515040918311250_cspp_dev.h5"},""" +
    """ {"uri": "/my_dir/SVI03_j02_d20230515_t0402523_e0404152_b02637_c20230515040918417310_cspp_dev.h5",""" +
    """ "uid": "SVI03_j02_d20230515_t0402523_e0404152_b02637_c20230515040918417310_cspp_dev.h5"},""" +
    """ {"uri": "/my_dir/SVI04_j02_d20230515_t0402523_e0404152_b02637_c20230515040918522149_cspp_dev.h5",""" +
    """ "uid": "SVI04_j02_d20230515_t0402523_e0404152_b02637_c20230515040918522149_cspp_dev.h5"},""" +
    """ {"uri": "/my_dir/SVI05_j02_d20230515_t0402523_e0404152_b02637_c20230515040918632921_cspp_dev.h5",""" +
    """ "uid": "SVI05_j02_d20230515_t0402523_e0404152_b02637_c20230515040918632921_cspp_dev.h5"},""" +
    """ {"uri": "/my_dir/GDNBO_j02_d20230515_t0402523_e0404152_b02637_c20230515040841929317_cspp_dev.h5",""" +
    """ "uid": "GDNBO_j02_d20230515_t0402523_e0404152_b02637_c20230515040841929317_cspp_dev.h5"},""" +
    """ {"uri": "/my_dir/SVDNB_j02_d20230515_t0402523_e0404152_b02637_c20230515040917932681_cspp_dev.h5",""" +
    """ "uid": "SVDNB_j02_d20230515_t0402523_e0404152_b02637_c20230515040917932681_cspp_dev.h5"},""" +
    """ {"uri": "/my_dir/IVCDB_j02_d20230515_t0402523_e0404152_b02637_c20230515040918055123_cspp_dev.h5",""" +
    """ "uid": "IVCDB_j02_d20230515_t0402523_e0404152_b02637_c20230515040918055123_cspp_dev.h5"},""" +
    """ {"uri": "/my_dir/lvl2/S_NWC_CMA_noaa21_00000_20230515T0402523Z_20230515T0404152Z.nc",""" +
    """ "uid": "S_NWC_CMA_noaa21_00000_20230515T0402523Z_20230515T0404152Z.nc"},""" +
    """ {"uri": "/my_dir/lvl2/S_NWC_CTTH_noaa21_00000_20230515T0402523Z_20230515T0404152Z.nc",""" +
    """ "uid": "S_NWC_CTTH_noaa21_00000_20230515T0402523Z_20230515T0404152Z.nc"},""" +
    """ {"uri": "/my_dir/lvl2/S_NWC_CT_noaa21_00000_20230515T0402523Z_20230515T0404152Z.nc",""" +
    """ "uid": "S_NWC_CT_noaa21_00000_20230515T0402523Z_20230515T0404152Z.nc"},""" +
    """ {"uri": "/my_dir/lvl2/S_NWC_CMIC_noaa21_00000_20230515T0402523Z_20230515T0404152Z.nc",""" +
    """ "uid": "S_NWC_CMIC_noaa21_00000_20230515T0402523Z_20230515T0404152Z.nc"},""" +
    """ {"uri": "/my_dir/lvl2/S_NWC_CMAPROB_noaa21_00000_20230515T0402523Z_20230515T0404152Z.nc",""" +
    """ "uid": "S_NWC_CMAPROB_noaa21_00000_20230515T0402523Z_20230515T0404152Z.nc"}],""" +
    """ "start_time": "2023-05-15T04:02:52.300000",""" +
    """ "end_time": "2023-05-15T04:04:15.200000"},""" +
    """ {"dataset": [""" +
    """ {"uri": "/my_dir/GMODO_j02_d20230515_t0404164_e0405411_b02637_c20230515041053656421_cspp_dev.h5",""" +
    """ "uid": "GMODO_j02_d20230515_t0404164_e0405411_b02637_c20230515041053656421_cspp_dev.h5"},""" +
    """ {"uri": "/my_dir/GMTCO_j02_d20230515_t0404164_e0405411_b02637_c20230515041053605930_cspp_dev.h5",""" +
    """ "uid": "GMTCO_j02_d20230515_t0404164_e0405411_b02637_c20230515041053605930_cspp_dev.h5"},""" +
    """ {"uri": "/my_dir/SVM01_j02_d20230515_t0404164_e0405411_b02637_c20230515041125283206_cspp_dev.h5",""" +
    """ "uid": "SVM01_j02_d20230515_t0404164_e0405411_b02637_c20230515041125283206_cspp_dev.h5"},""" +
    """ {"uri": "/my_dir/SVM02_j02_d20230515_t0404164_e0405411_b02637_c20230515041125323663_cspp_dev.h5",""" +
    """ "uid": "SVM02_j02_d20230515_t0404164_e0405411_b02637_c20230515041125323663_cspp_dev.h5"},""" +
    """ {"uri": "/my_dir/SVM03_j02_d20230515_t0404164_e0405411_b02637_c20230515041125368728_cspp_dev.h5",""" +
    """ "uid": "SVM03_j02_d20230515_t0404164_e0405411_b02637_c20230515041125368728_cspp_dev.h5"},""" +
    """ {"uri": "/my_dir/SVM04_j02_d20230515_t0404164_e0405411_b02637_c20230515041125417060_cspp_dev.h5",""" +
    """ "uid": "SVM04_j02_d20230515_t0404164_e0405411_b02637_c20230515041125417060_cspp_dev.h5"},""" +
    """ {"uri": "/my_dir/SVM05_j02_d20230515_t0404164_e0405411_b02637_c20230515041125464882_cspp_dev.h5",""" +
    """ "uid": "SVM05_j02_d20230515_t0404164_e0405411_b02637_c20230515041125464882_cspp_dev.h5"},""" +
    """ {"uri": "/my_dir/SVM06_j02_d20230515_t0404164_e0405411_b02637_c20230515041125507702_cspp_dev.h5",""" +
    """ "uid": "SVM06_j02_d20230515_t0404164_e0405411_b02637_c20230515041125507702_cspp_dev.h5"},""" +
    """ {"uri": "/my_dir/SVM07_j02_d20230515_t0404164_e0405411_b02637_c20230515041125550239_cspp_dev.h5",""" +
    """ "uid": "SVM07_j02_d20230515_t0404164_e0405411_b02637_c20230515041125550239_cspp_dev.h5"},""" +
    """ {"uri": "/my_dir/SVM08_j02_d20230515_t0404164_e0405411_b02637_c20230515041125593334_cspp_dev.h5",""" +
    """ "uid": "SVM08_j02_d20230515_t0404164_e0405411_b02637_c20230515041125593334_cspp_dev.h5"},""" +
    """ {"uri": "/my_dir/SVM09_j02_d20230515_t0404164_e0405411_b02637_c20230515041125633718_cspp_dev.h5",""" +
    """ "uid": "SVM09_j02_d20230515_t0404164_e0405411_b02637_c20230515041125633718_cspp_dev.h5"},""" +
    """ {"uri": "/my_dir/SVM10_j02_d20230515_t0404164_e0405411_b02637_c20230515041125677125_cspp_dev.h5",""" +
    """ "uid": "SVM10_j02_d20230515_t0404164_e0405411_b02637_c20230515041125677125_cspp_dev.h5"},""" +
    """ {"uri": "/my_dir/SVM11_j02_d20230515_t0404164_e0405411_b02637_c20230515041125718392_cspp_dev.h5",""" +
    """ "uid": "SVM11_j02_d20230515_t0404164_e0405411_b02637_c20230515041125718392_cspp_dev.h5"},""" +
    """ {"uri": "/my_dir/SVM12_j02_d20230515_t0404164_e0405411_b02637_c20230515041125757076_cspp_dev.h5",""" +
    """ "uid": "SVM12_j02_d20230515_t0404164_e0405411_b02637_c20230515041125757076_cspp_dev.h5"},""" +
    """ {"uri": "/my_dir/SVM13_j02_d20230515_t0404164_e0405411_b02637_c20230515041125781624_cspp_dev.h5",""" +
    """ "uid": "SVM13_j02_d20230515_t0404164_e0405411_b02637_c20230515041125781624_cspp_dev.h5"},""" +
    """ {"uri": "/my_dir/SVM14_j02_d20230515_t0404164_e0405411_b02637_c20230515041125826785_cspp_dev.h5",""" +
    """ "uid": "SVM14_j02_d20230515_t0404164_e0405411_b02637_c20230515041125826785_cspp_dev.h5"},""" +
    """ {"uri": "/my_dir/SVM15_j02_d20230515_t0404164_e0405411_b02637_c20230515041125868575_cspp_dev.h5",""" +
    """ "uid": "SVM15_j02_d20230515_t0404164_e0405411_b02637_c20230515041125868575_cspp_dev.h5"},""" +
    """ {"uri": "/my_dir/SVM16_j02_d20230515_t0404164_e0405411_b02637_c20230515041125908912_cspp_dev.h5",""" +
    """ "uid": "SVM16_j02_d20230515_t0404164_e0405411_b02637_c20230515041125908912_cspp_dev.h5"},""" +
    """ {"uri": "/my_dir/GIMGO_j02_d20230515_t0404164_e0405411_b02637_c20230515041053201130_cspp_dev.h5",""" +
    """ "uid": "GIMGO_j02_d20230515_t0404164_e0405411_b02637_c20230515041053201130_cspp_dev.h5"},""" +
    """ {"uri": "/my_dir/GITCO_j02_d20230515_t0404164_e0405411_b02637_c20230515041053020517_cspp_dev.h5",""" +
    """ "uid": "GITCO_j02_d20230515_t0404164_e0405411_b02637_c20230515041053020517_cspp_dev.h5"},""" +
    """ {"uri": "/my_dir/SVI01_j02_d20230515_t0404164_e0405411_b02637_c20230515041124875834_cspp_dev.h5",""" +
    """ "uid": "SVI01_j02_d20230515_t0404164_e0405411_b02637_c20230515041124875834_cspp_dev.h5"},""" +
    """ {"uri": "/my_dir/SVI02_j02_d20230515_t0404164_e0405411_b02637_c20230515041124975813_cspp_dev.h5",""" +
    """ "uid": "SVI02_j02_d20230515_t0404164_e0405411_b02637_c20230515041124975813_cspp_dev.h5"},""" +
    """ {"uri": "/my_dir/SVI03_j02_d20230515_t0404164_e0405411_b02637_c20230515041125069217_cspp_dev.h5",""" +
    """ "uid": "SVI03_j02_d20230515_t0404164_e0405411_b02637_c20230515041125069217_cspp_dev.h5"},""" +
    """ {"uri": "/my_dir/SVI04_j02_d20230515_t0404164_e0405411_b02637_c20230515041125155142_cspp_dev.h5",""" +
    """ "uid": "SVI04_j02_d20230515_t0404164_e0405411_b02637_c20230515041125155142_cspp_dev.h5"},""" +
    """ {"uri": "/my_dir/SVI05_j02_d20230515_t0404164_e0405411_b02637_c20230515041125244732_cspp_dev.h5",""" +
    """ "uid": "SVI05_j02_d20230515_t0404164_e0405411_b02637_c20230515041125244732_cspp_dev.h5"},""" +
    """ {"uri": "/my_dir/GDNBO_j02_d20230515_t0404164_e0405411_b02637_c20230515041052831937_cspp_dev.h5",""" +
    """ "uid": "GDNBO_j02_d20230515_t0404164_e0405411_b02637_c20230515041052831937_cspp_dev.h5"},""" +
    """ {"uri": "/my_dir/SVDNB_j02_d20230515_t0404164_e0405411_b02637_c20230515041124725653_cspp_dev.h5",""" +
    """ "uid": "SVDNB_j02_d20230515_t0404164_e0405411_b02637_c20230515041124725653_cspp_dev.h5"},""" +
    """ {"uri": "/my_dir/IVCDB_j02_d20230515_t0404164_e0405411_b02637_c20230515041124790139_cspp_dev.h5",""" +
    """ "uid": "IVCDB_j02_d20230515_t0404164_e0405411_b02637_c20230515041124790139_cspp_dev.h5"},""" +
    """ {"uri": "/my_dir/lvl2/S_NWC_CMA_noaa21_00000_20230515T0404164Z_20230515T0405411Z.nc",""" +
    """ "uid": "S_NWC_CMA_noaa21_00000_20230515T0404164Z_20230515T0405411Z.nc"},""" +
    """ {"uri": "/my_dir/lvl2/S_NWC_CTTH_noaa21_00000_20230515T0404164Z_20230515T0405411Z.nc",""" +
    """ "uid": "S_NWC_CTTH_noaa21_00000_20230515T0404164Z_20230515T0405411Z.nc"},""" +
    """ {"uri": "/my_dir/lvl2/S_NWC_CT_noaa21_00000_20230515T0404164Z_20230515T0405411Z.nc",""" +
    """ "uid": "S_NWC_CT_noaa21_00000_20230515T0404164Z_20230515T0405411Z.nc"},""" +
    """ {"uri": "/my_dir/lvl2/S_NWC_CMIC_noaa21_00000_20230515T0404164Z_20230515T0405411Z.nc",""" +
    """ "uid": "S_NWC_CMIC_noaa21_00000_20230515T0404164Z_20230515T0405411Z.nc"},""" +
    """ {"uri": "/my_dir/lvl2/S_NWC_CMAPROB_noaa21_00000_20230515T0404164Z_20230515T0405411Z.nc", """ +
    """ "uid": "S_NWC_CMAPROB_noaa21_00000_20230515T0404164Z_20230515T0405411Z.nc"}], """ +
    """ "start_time": "2023-05-15T04:04:16.400000", "end_time": "2023-05-15T04:05:41.100000"}]}""")


TEST_PPS_COLLECTOR_OK = """

subscribe_topics: [/collection/SDR+CF/1+2/CloudProducts]
publish_topic: NWCSAFPPS/2+1C/collection
pps_lvl1c_dir: my_test_dir

"""


@pytest.fixture
def fake_file(tmp_path):
    """Create directory with test files."""
    file_cfg = tmp_path / 'pps_collector_config.yaml'
    file_h = open(file_cfg, 'w')
    file_h.write(TEST_PPS_COLLECTOR_OK.replace("my_test_dir", str(tmp_path)))
    file_h.close()
    return str(file_cfg)


class TestPpsCollector:
    """Test the pps collector."""

    # def test_pps_collector_runner(self, fake_file):
    #    myconfig_filename = fake_file
    #    input_msg = Message.decode(rawstr=TEST_INPUT_MSG)
    #    messages = [input_msg]
    #    subscriber_settings = dict(nameserver=False, addresses=["ipc://bla"])
    #    with patched_subscriber_recv(messages):
    #        pps_collector_runner(myconfig_filename)

    def test_prepare_pps_collector_message(self, fake_file):
        """Test that meesage is prepared correctly."""
        myconfig_filename = fake_file
        options = get_config(myconfig_filename)
        input_msg = Message.decode(rawstr=TEST_INPUT_MSG)
        output_msg = prepare_pps_collector_message(input_msg, options)
        level1c_file_included = False
        for index in [0, 1]:
            level1c_file_included = False
            for item in output_msg["collection"][index]['dataset']:
                assert "S_NWC" in item["uid"]
                if "S_NWC_viirs" in item["uid"]:
                    level1c_file_included = True
            assert level1c_file_included
