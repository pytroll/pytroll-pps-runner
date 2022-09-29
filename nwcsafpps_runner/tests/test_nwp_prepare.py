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

"""Unit testing the nwp_prepare runner code."""

from unittest.mock import patch
import unittest
from datetime import datetime
import os
import logging
from importlib import reload
from datetime import timedelta
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
        self.requirement_name = my_tmp_dir + '/pps_nwp_req.txt'
        req_file = open(self.requirement_name, 'w')
        req_file.write("M 235 Skin temperature 0 surface\n" +
                       "O 129 Geopotential 350 isobaricInhPa\n")
        req_file.close()
        self.requirement_name_m = my_tmp_dir + '/pps_nwp_req_mandatory.txt'
        req_file = open(self.requirement_name_m, 'w')
        req_file.write("M 235 Skin temperature 0 surface\n" +
                       "M 129 Geopotential 350 isobaricInhPa\n")
        req_file.close()
        self.OPTIONS = {
            "pps_nwp_requirements": self.requirement_name,
            "nwp_outdir": my_tmp_dir,
            "nhsp_path": "nwcsafpps_runner/tests/files/",
            "nhsf_path": "nwcsafpps_runner/tests/files/",
            "nhsp_prefix": "LL02_NHSP_",
            "nhsf_prefix": "LL02_NHSF_",
            "nwp_static_surface": my_tmp_dir + '/empty_file',
            "ecmwf_prefix": "LL02_NHSF",
            "nwp_output_prefix": "PPS_ECMWF_",
            "nhsf_file_name_sift": '{ecmwf_prefix:9s}_{analysis_time:%Y%m%d%H%M}+{forecast_step:d}H00M'
        }
        self.OPTIONS_M = dict(self.OPTIONS)
        self.OPTIONS_M["pps_nwp_requirements"] = self.requirement_name_m
        self.OPTIONS_M["nwp_output_prefix"] = "PPS_ECMWF_MANDATORY_"
        self.outfile = my_tmp_dir + "/PPS_ECMWF_202205100000+009H00M"
        fhand = open(self.OPTIONS["nwp_static_surface"], 'a')
        fhand.close()

    @patch('nwcsafpps_runner.config.get_config')
    def test_update_nwp(self, mock_get_config):
        """Create file."""
        mock_get_config.return_value = self.OPTIONS
        import nwcsafpps_runner.prepare_nwp as nwc_prep
        reload(nwc_prep)
        date = datetime(year=2022, month=5, day=10, hour=0)
        nwc_prep.update_nwp(date - timedelta(days=2), [9])
        # Run again when file is already created
        nwc_prep.update_nwp(date - timedelta(days=2), [9])
        self.assertTrue(os.path.exists(self.outfile))

    @patch('nwcsafpps_runner.config.get_config')
    def test_update_nwp_no_config_file(self, mock_get_config):
        """Create file no config file."""
        mock_get_config.return_value = self.OPTIONS
        os.remove(self.requirement_name)
        import nwcsafpps_runner.prepare_nwp as nwc_prep
        reload(nwc_prep)
        date = datetime(year=2022, month=5, day=10, hour=0)
        nwc_prep.update_nwp(date - timedelta(days=2), [9])
        self.assertTrue(os.path.exists(self.outfile))

    @patch('nwcsafpps_runner.config.get_config')
    def test_update_nwp_missing_fields(self, mock_get_config):
        """Test that no file without mandatory data is created."""
        mock_get_config.return_value = self.OPTIONS_M
        import nwcsafpps_runner.prepare_nwp as nwc_prep
        reload(nwc_prep)
        date = datetime(year=2022, month=5, day=10, hour=0)
        nwc_prep.update_nwp(date - timedelta(days=2), [9])
        self.assertFalse(os.path.exists("temp_test_dir/PPS_ECMWF_MANDATORY_202205100000+009H00M"))
        os.remove(self.OPTIONS["nwp_static_surface"])
        os.remove(self.requirement_name_m)

    @patch('nwcsafpps_runner.config.get_config')
    def test_remove_filename(self, mock_get_config):
        """Test the function for removing files."""
        from nwcsafpps_runner.prepare_nwp import remove_file
        mock_get_config.return_value = self.OPTIONS
        remove_file(self.OPTIONS["nwp_static_surface"])
        self.assertFalse(os.path.exists(self.OPTIONS["nwp_static_surface"]))
        # Should be able to run on already removed file without raising exception
        remove_file(self.OPTIONS["nwp_static_surface"])

    def tearDown(self):
        """Remove files after testing."""
        for temp_file in [self.OPTIONS["nwp_static_surface"], self.requirement_name_m,
                          self.requirement_name, self.outfile]:
            if os.path.exists(temp_file):
                os.remove(temp_file)


def suite():
    """Create the test suite for test_nwp_prepare."""
    loader = unittest.TestLoader()
    mysuite = unittest.TestSuite()
    mysuite.addTest(loader.loadTestsFromTestCase(NWPprepareRunner))

    return mysuite
