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
import pytest
import unittest
from datetime import datetime
import os
import logging
from datetime import timedelta
from nwcsafpps_runner.message_utils import prepare_nwp_message
import nwcsafpps_runner.prepare_nwp as nwc_prep

LOG = logging.getLogger(__name__)
logging.basicConfig(
    format='%(levelname)s |%(asctime)s|: %(message)s',
    level=logging.DEBUG,
    # datefmt='%Y-%m-%d %H:%M:%S')
    datefmt='%H:%M:%S')


@pytest.fixture
def fake_file_dir(tmp_path):
    """Create directory with test files."""
    my_temp_dir = tmp_path / "temp_test_dir"
    my_temp_dir.mkdir()
    my_temp_dir = my_temp_dir

    requirement_name = my_temp_dir / 'pps_nwp_req.txt'
    req_file = open(requirement_name, 'w')
    req_file.write("M 235 Skin temperature 0 surface\n" +
                   "O 129 Geopotential 350 isobaricInhPa\n")
    req_file.close()
    requirement_name_m = my_temp_dir / 'pps_nwp_req_mandatory.txt'
    req_file = open(requirement_name_m, 'w')
    req_file.write("M 235 Skin temperature 0 surface\n" +
                   "M 129 Geopotential 350 isobaricInhPa\n")
    req_file.close()
    static_surface = my_temp_dir / 'static_surface'
    req_file = open(static_surface, 'a')
    req_file.close()

    cfg_file = my_temp_dir / 'pps_config.yaml'
    req_file = open(cfg_file, 'w')
    req_file.write(
        "pps_nwp_requirements: " + str(requirement_name) + "\n"
        "nwp_outdir: " + str(my_temp_dir) + "\n"
        "nhsp_path: " + "nwcsafpps_runner/tests/files/" + "\n"
        "nhsf_path: " + "nwcsafpps_runner/tests/files/" + "\n"
        "nhsp_prefix: " + "LL02_NHSP_" + "\n"
        "nhsf_prefix: " + "LL02_NHSF_" + "\n"
        "nwp_static_surface: " + str(my_temp_dir) + "/static_surface" + "\n"
        "ecmwf_prefix: " + "LL02_NHSF" + "\n"
        "nwp_output_prefix: " + "PPS_ECMWF_" + "\n"
        "nhsf_file_name_sift: '" + '{ecmwf_prefix:9s}_{analysis_time:%Y%m%d%H%M}+{forecast_step:d}H00M' + "'" + "\n")

    cfg_file = my_temp_dir / 'pps_config_missing_fields.yaml'
    req_file = open(cfg_file, 'w')
    req_file.write(
        "pps_nwp_requirements: " + str(requirement_name_m) + "\n"
        "nwp_outdir: " + str(my_temp_dir) + "\n"
        "nhsp_path: " + "nwcsafpps_runner/tests/files/" + "\n"
        "nhsf_path: " + "nwcsafpps_runner/tests/files/" + "\n"
        "nhsp_prefix: " + "LL02_NHSP_" + "\n"
        "nhsf_prefix: " + "LL02_NHSF_" + "\n"
        "nwp_static_surface: " + str(my_temp_dir) + "/static_surface" + "\n"
        "ecmwf_prefix: " + "LL02_NHSF" + "\n"
        "nwp_output_prefix: " + "PPS_ECMWF_MANDATORY" + "\n"
        "nhsf_file_name_sift: '" + '{ecmwf_prefix:9s}_{analysis_time:%Y%m%d%H%M}+{forecast_step:d}H00M' + "'" + "\n")

    return str(my_temp_dir)


class TestNwpMessage:
    """Test the nwp message."""

    def test_nwp_message(self):
        """Test the nwp message."""
        filename = "dummy_dir/PPS_ECMWF_202205100000+009H00M"
        publish_msg = prepare_nwp_message(filename, "dummy_topic")
        expected_uri = '"uri": "dummy_dir/PPS_ECMWF_202205100000+009H00M"'
        assert expected_uri in publish_msg


class TestNWPprepareRunner:
    """Test the nwp prepare runer."""

    def test_update_nwp(self, fake_file_dir):
        """Test create file."""
        my_temp_dir = fake_file_dir
        outfile = os.path.join(str(my_temp_dir), "PPS_ECMWF_202205100000+009H00M")
        cfg_file = my_temp_dir + '/pps_config.yaml'
        date = datetime(year=2022, month=5, day=10, hour=0)
        nwc_prep.update_nwp(date - timedelta(days=2), [9], cfg_file)
        # Run again when file is already created
        nwc_prep.update_nwp(date - timedelta(days=2), [9], cfg_file)
        assert os.path.exists(outfile)

    def test_update_nwp_no_requirement_file(self, fake_file_dir):
        """Create file no requirement file."""
        my_temp_dir = fake_file_dir
        cfg_file = my_temp_dir + '/pps_config.yaml'
        requirement_name = str(my_temp_dir) + '/pps_nwp_req.txt'
        outfile = os.path.join(str(my_temp_dir), "PPS_ECMWF_202205100000+009H00M")
        os.remove(requirement_name)
        date = datetime(year=2022, month=5, day=10, hour=0)
        nwc_prep.update_nwp(date - timedelta(days=2), [9], cfg_file)
        assert os.path.exists(outfile)

    def test_update_nwp_missing_fields(self, fake_file_dir):
        """Test that no file without mandatory data is created."""
        my_temp_dir = fake_file_dir
        outfile = os.path.join(str(my_temp_dir), "PPS_ECMWF_MANDATORY_202205100000+009H00M")
        cfg_file = my_temp_dir + '/pps_config_missing_fields.yaml'
        date = datetime(year=2022, month=5, day=10, hour=0)
        nwc_prep.update_nwp(date - timedelta(days=2), [9], cfg_file)
        date = datetime(year=2022, month=5, day=10, hour=0)
        nwc_prep.update_nwp(date - timedelta(days=2), [9], cfg_file)
        assert not (os.path.exists(outfile))

    def test_remove_filename(self, fake_file_dir):
        """Test the function for removing files."""
        from nwcsafpps_runner.prepare_nwp import remove_file
        my_temp_dir = fake_file_dir
        nwp_surface_file = str(my_temp_dir) + '/static_surface'
        remove_file(nwp_surface_file)
        assert not os.path.exists(nwp_surface_file)
        # Should be able to run on already removed file without raising exception
        remove_file(nwp_surface_file)
