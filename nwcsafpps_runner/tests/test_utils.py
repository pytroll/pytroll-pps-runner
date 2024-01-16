#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (c) 2020 - 2022 pps2018_runner developers
#
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

"""Test utility functions."""
import os
import tempfile
import unittest
import pytest
from unittest.mock import patch, MagicMock
from datetime import datetime
from nwcsafpps_runner.utils import (create_xml_timestat_from_lvl1c,
                                    find_product_statistics_from_lvl1c,
                                    publish_pps_files)
from nwcsafpps_runner.utils import FindTimeControlFileError


@pytest.fixture
def fake_file_dir(tmp_path):
    """Create directory with test files."""
    mydir = tmp_path / "import"
    mydir.mkdir()
    mydir_out = tmp_path / "export"
    mydir_out.mkdir()
    mydir = mydir
    mydir_out = mydir_out

    def create_files(mydir, file_tag, typ):
        #: These files should always be found
        f1 = mydir / "S_NWC_{}_npp_12345_19810305T0715000Z_19810305T0730000Z{}".format(file_tag, typ)
        f1.write_text("test_file type {:s}".format(typ))
        f1 = mydir / "S_NWC_{}_noaa20_12345_19810305T0715000Z_19810305T0730000Z{}".format(file_tag, typ)
        f1.write_text("test_file type {:s}".format(typ))
        f1 = mydir / "S_NWC_{}_npp_82345_19820305T0715000Z_19820305T0730000Z{}".format(file_tag, typ)
        f1.write_text("test_file type {:s}".format(typ))

    #: Create the level1c and xml files
    create_files(mydir, "viirs", ".nc")
    create_files(mydir_out, "CMAPROB", ".nc")
    create_files(mydir_out, "CMAPROB", "_statistics.xml")
    create_files(mydir_out, "CMIC", "_statistics.xml")
    create_files(mydir_out, "CTTH", "_statistics.xml")
    create_files(mydir_out, "timectrl", ".txt")
    create_files(mydir_out, "timectrl", ".xml")
    create_files(mydir_out, "timectrl", "_dummy.xml")
    return mydir, mydir_out


class TestCreateXmlFromLvl1c:
    """Test finding xml files form level1c file."""

    def setup(self):
        """Define the level1c filename."""
        self.scene = {'file4pps': "S_NWC_viirs_npp_12345_19810305T0715000Z_19810305T0730000Z.nc",
                      'starttime': datetime.strptime('19810305T0715000', "%Y%m%dT%H%M%S%f"),
                      'orbit_number': 12345,
                      'platform_name': 'Suomi-NPP'}
        self.empty_scene = {}

    def test_xml_for_timectrl(self, fake_file_dir):
        """Test xml files for timectrl."""
        mydir, mydir_out = fake_file_dir
        mymodule = MagicMock()
        import sys
        sys.modules["pps_time_control"] = mymodule
        res = create_xml_timestat_from_lvl1c(self.scene, mydir_out)
        expected = [os.path.join(mydir_out, "S_NWC_timectrl_npp_12345_19810305T0715000Z_19810305T0730000Z.xml")]
        assert len(res) == len(set(expected))
        assert set(res) == set(expected)

    def test_xml_for_timectrl_files_missing(self, fake_file_dir):
        """Test xml files for timectrl."""
        mydir, mydir_out = fake_file_dir
        mymodule = MagicMock()
        import sys
        sys.modules["pps_time_control"] = mymodule
        res = create_xml_timestat_from_lvl1c(self.scene, mydir)  # Look in wrong place
        expected = []
        assert res == expected

    def test_xml_for_products(self, fake_file_dir):
        """Test xml files for products statistics files."""
        mydir, mydir_out = fake_file_dir
        res = find_product_statistics_from_lvl1c(self.scene, mydir_out)
        expected = [
            os.path.join(mydir_out, "S_NWC_CMAPROB_npp_12345_19810305T0715000Z_19810305T0730000Z_statistics.xml"),
            os.path.join(mydir_out, "S_NWC_CTTH_npp_12345_19810305T0715000Z_19810305T0730000Z_statistics.xml"),
            os.path.join(mydir_out, "S_NWC_CMIC_npp_12345_19810305T0715000Z_19810305T0730000Z_statistics.xml")]
        assert len(res) == len(set(expected))
        assert set(res) == set(expected)

    def test_xml_for_timectrl_no_file4pps(self, fake_file_dir):
        """Test xml files for timectrl without file4pps attribute."""
        mydir, mydir_out = fake_file_dir
        res = create_xml_timestat_from_lvl1c(self.empty_scene, mydir_out)
        assert res == []

    def test_xml_for_products_no_file4pps(self, fake_file_dir):
        """Test xml files for products without file4pps attribute."""
        mydir, mydir_out = fake_file_dir
        res = find_product_statistics_from_lvl1c(self.empty_scene, mydir_out)
        assert res == []


class TestPublishPPSFiles(unittest.TestCase):
    """Test publish pps files."""

    def test_publish_pps_files(self):
        """Test publish pps files."""
        from posttroll.message import Message
        from multiprocessing import Manager
        file1 = "S_NWC_CTTH_metopb_46878_20210930T0947019Z_20210930T1001458Z_statistics.xml"
        file2 = "S_NWC_CMA_metopb_46878_20210930T0947019Z_20210930T1001458Z_statistics.xml"
        scene = {'instrument': 'avhrr', 'platform_name': 'EOS-Terra', 'orbit_number': "46878"}
        input_msg = Message(data={'dataset': 'dummy'}, atype='dataset', subject='test')
        result_files = [file1, file2]
        manager = Manager()
        publish_q = manager.Queue()
        publish_q.put = MagicMock()
        publish_pps_files(input_msg, publish_q, scene, result_files)
        msg_out = publish_q.put.call_args_list
        self.assertTrue(file2 in msg_out[1].args[0])
        self.assertTrue(file1 in msg_out[0].args[0])


if __name__ == "__main__":
    pass
