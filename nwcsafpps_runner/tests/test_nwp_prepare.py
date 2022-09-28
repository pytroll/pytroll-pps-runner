#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2021 Adam.Dybbroe

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

"""Unit testing the nwp_prepare runner code."""

from unittest.mock import patch
import unittest
from datetime import datetime
import os
import logging
import pygrib
LOG = logging.getLogger(__name__)
logging.basicConfig(
    format='%(levelname)s |%(asctime)s|: %(message)s',
    level=logging.DEBUG,
    # datefmt='%Y-%m-%d %H:%M:%S')
    datefmt='%H:%M:%S')


class NWPprepareRunner(unittest.TestCase):
    """Test the nwp prepare runer."""

    def setUp(self):
        """Create config options and file."""
        my_tmp_dir = "temp_test_dir"
        if os.path.exists(my_tmp_dir):
            pass
        else:
            os.mkdir(my_tmp_dir)
        self.default_config_name = my_tmp_dir + '/pps2018_config.yaml'
        req_file = open(self.default_config_name, 'w')
        req_file.write("M 235 Skin temperature 0 surface\n" +
                       "O 129 Geopotential 350 isobaricInhPa\n")
        req_file.close()
        self.OPTIONS = {
            "pps_nwp_requirements": my_tmp_dir + '/pps2018_config.yaml',
            "nwp_outdir": my_tmp_dir,
            "nhsp_path": "nwcsafpps_runner/tests/files/",
            "nhsf_path": "nwcsafpps_runner/tests/files/",
            "nhsp_prefix": "LL02_NHSP_",
            "nhsf_prefix": "LL02_NHSF_",
            "nwp_static_surface": my_tmp_dir + '/empty_file',
            "ecmwf_prefix": "LL02_NHSF",
            "nwp_output_prefix": "PPS_ECMWF",
            "nhsf_file_name_sift": '{ecmwf_prefix:9s}_{analysis_time:%Y%m%d%H%M}+{forecast_step:d}H00M'
        }

        fhand = open(self.OPTIONS["nwp_static_surface"], 'a')
        fhand.close()

    @patch('nwcsafpps_runner.config.get_config')
    def test_update_nwp(self, mock_get_config):
        """Create config options and file."""
        mock_get_config.return_value = self.OPTIONS
        from nwcsafpps_runner.prepare_nwp import update_nwp
        from datetime import timedelta
        date = datetime(year=2022, month=5, day=10, hour=0)

        update_nwp(date - timedelta(days=2), [9])
        self.assertTrue(os.path.exists("temp_test_dir/PPS_ECMWF202205100000+009H00M"))
        os.remove("temp_test_dir/PPS_ECMWF202205100000+009H00M")
        os.remove(self.default_config_name)
        os.remove(self.OPTIONS["nwp_static_surface"])


def suite():
    """Create the test suite for test_nwp_prepare."""
    loader = unittest.TestLoader()
    mysuite = unittest.TestSuite()
    mysuite.addTest(loader.loadTestsFromTestCase(NWPprepareRunner))

    return mysuite
