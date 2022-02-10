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
from datetime import datetime

from nwcsafpps_runner.utils import get_outputfiles
from nwcsafpps_runner.utils import get_time_control_ascii_filename_candidates
from nwcsafpps_runner.utils import get_time_control_ascii_filename
from nwcsafpps_runner.utils import FindTimeControlFileError
from nwcsafpps_runner.utils import get_product_statistics_files


def test_outputfiles(tmp_path):
    """Test get_outputfiles.

    get_outputfiles uses os.stat to test if a file is older than 90 min,
    and if so disregard it. This behaviour can't be tested at the moment.
    TODO: either one file (correct orbit number and start-time) needs to
    be created more than 90 mins ago or the os.stat should be modified
    so the so.stat thinks the file was created more than 90 mins ago.
    The file should than not be found.
    """
    #: Create temp_path
    mydir = tmp_path / "export"
    mydir.mkdir()

    #: Create test files
    def create_files(mydir, typ):
        #: These files should always be found
        f1 = mydir / "S_NWC_CMAPROB_noaa15_12345_19810305T0715000Z_19810305T0730000Z.{}".format(typ)
        f1.write_text("correct orbit and time")
        #: These files should be found if start time is not given
        f2 = mydir / "S_NWC_CMAPROB_noaa15_12345_19810305T0745000Z_19810305T0800000Z.{}".format(typ)
        f2.write_text("correct orbit and time within 90 min")
        #: These files should not be found although the start time is correct
        f3 = mydir / "S_NWC_CMAPROB_noaa15_54321_19810305T0715000Z_19810305T0730000Z.{}".format(typ)
        f3.write_text("wrong orbit and correct time")

    #: Test xml files without start time
    typ = "xml"
    create_files(mydir, typ)
    expected = [os.path.join(mydir, "S_NWC_CMAPROB_noaa15_12345_19810305T0715000Z_19810305T0730000Z.{}".format(typ)),
                os.path.join(mydir, "S_NWC_CMAPROB_noaa15_12345_19810305T0745000Z_19810305T0800000Z.{}".format(typ))]
    res = get_outputfiles(mydir, "noaa15", 12345, xml_output=True)
    assert len(res) == len(set(res))
    assert set(res) == set(expected)
    #: Test xml files with start time
    expected = [os.path.join(mydir, "S_NWC_CMAPROB_noaa15_12345_19810305T0715000Z_19810305T0730000Z.{}".format(typ))]
    res = get_outputfiles(mydir, "noaa15", 12345, st_time="19810305T0715", xml_output=True)
    assert len(res) == len(set(res))
    assert set(res) == set(expected)

    #: Test h5 files without start time
    typ = "h5"
    create_files(mydir, typ)
    expected = [os.path.join(mydir, "S_NWC_CMAPROB_noaa15_12345_19810305T0715000Z_19810305T0730000Z.{}".format(typ)),
                os.path.join(mydir, "S_NWC_CMAPROB_noaa15_12345_19810305T0745000Z_19810305T0800000Z.{}".format(typ))]
    res = get_outputfiles(mydir, "noaa15", 12345, h5_output=True)
    assert len(res) == len(set(res))
    assert set(res) == set(expected)
    #: Test h5 files with start time
    expected = [os.path.join(mydir, "S_NWC_CMAPROB_noaa15_12345_19810305T0715000Z_19810305T0730000Z.{}".format(typ))]
    res = get_outputfiles(mydir, "noaa15", 12345, st_time="19810305T0715", h5_output=True)
    assert len(res) == len(set(res))
    assert set(res) == set(expected)

    #: Test nc files without start time
    typ = "nc"
    create_files(mydir, typ)
    expected = [os.path.join(mydir, "S_NWC_CMAPROB_noaa15_12345_19810305T0715000Z_19810305T0730000Z.{}".format(typ)),
                os.path.join(mydir, "S_NWC_CMAPROB_noaa15_12345_19810305T0745000Z_19810305T0800000Z.{}".format(typ))]
    res = get_outputfiles(mydir, "noaa15", 12345, nc_output=True)
    assert len(res) == len(set(res))
    assert set(res) == set(expected)
    #: Test nc files with start time
    expected = [os.path.join(mydir, "S_NWC_CMAPROB_noaa15_12345_19810305T0715000Z_19810305T0730000Z.{}".format(typ))]
    res = get_outputfiles(mydir, "noaa15", 12345, st_time="19810305T0715", nc_output=True)
    assert len(res) == len(set(res))
    assert set(res) == set(expected)


class TestTimeControlFiles(unittest.TestCase):
    """Testing the handling and generation of time control XML files."""

    def setUp(self):
        self.testscene = {'platform_name': 'Metop-B', 'orbit_number': 46878,
                          'satday': '20210930', 'sathour': '0946',
                          'starttime': datetime(2021, 9, 30, 9, 46, 24),
                          'endtime': datetime(2021, 9, 30, 10, 1, 43),
                          'sensor': ['avhrr/3', 'mhs', 'amsu-a'],
                          'file4pps': '/data/metop01_20210930_0946_46878/hrpt_metop01_20210930_0946_46878.l1b'}
        self.filename1 = 'S_NWC_timectrl_metopb_46878_20210930T0946289Z_20210930T1001458Z.txt'
        self.filename2 = 'S_NWC_timectrl_metopb_46878_20210930T0949289Z_20210930T1001459Z.txt'
        self.filename3 = 'S_NWC_timectrl_metopb_46878_20210930T0945289Z_20210930T1001459Z.txt'
        self.filename_timeoff = 'S_NWC_timectrl_metopb_46878_20210930T0950000Z_20210930T1001459Z.txt'

        self.modis_scene = {'platform_name': 'EOS-Aqua', 'orbit_number': 5161,
                            'satday': '20220209', 'sathour': '2210',
                            'starttime': datetime(2022, 2, 9, 22, 10, 11),
                            'endtime': datetime(2022, 2, 9, 22, 14, 41),
                            'sensor': 'modis',
                            'file4pps': '/data/eos/lvl1/MYD021km_A22040_221011_2022040221451.hdf'}
        self.filename1_modis = 'S_NWC_timectrl_eos2_00000_20220209T2210110Z_20220209T2212284Z.txt'

    def test_get_time_control_ascii_filename_ok(self):

        with tempfile.TemporaryDirectory() as tmpdirname:

            self.file1 = os.path.join(tmpdirname, self.filename1)
            with open(self.file1, 'w') as _:
                pass
            self.file2 = os.path.join(tmpdirname, self.filename2)
            with open(self.file2, 'w') as _:
                pass

            ascii_file = get_time_control_ascii_filename(self.testscene, tmpdirname)

        self.assertEqual(os.path.basename(ascii_file),
                         'S_NWC_timectrl_metopb_46878_20210930T0946289Z_20210930T1001458Z.txt')

    def test_get_time_control_ascii_filename_more_than_one_file(self):

        with tempfile.TemporaryDirectory() as tmpdirname:

            self.file1 = os.path.join(tmpdirname, self.filename1)
            with open(self.file1, 'w') as _:
                pass
            self.file2 = os.path.join(tmpdirname, self.filename3)
            with open(self.file2, 'w') as _:
                pass

            with pytest.raises(FindTimeControlFileError) as exec_info:
                ascii_file = get_time_control_ascii_filename(self.testscene, tmpdirname)

            expected = 'More than one time control ascii file candidate found - unresolved ambiguity!'
            assert str(exec_info.value) == expected

    def test_get_time_control_ascii_filename_no_files(self):

        with tempfile.TemporaryDirectory() as tmpdirname:

            self.file1 = os.path.join(tmpdirname, self.filename2)
            with open(self.file1, 'w') as _:
                pass

            with pytest.raises(FindTimeControlFileError) as exec_info:
                ascii_file = get_time_control_ascii_filename(self.testscene, tmpdirname)

            expected = 'No time control ascii file candidate found!'
            assert str(exec_info.value) == expected

    def test_get_time_control_ascii_filename_candidates_orbit_ok_time_off(self):

        with tempfile.TemporaryDirectory() as tmpdirname:

            self.file1 = os.path.join(tmpdirname, self.filename_timeoff)
            with open(self.file1, 'w') as _:
                pass

            ascii_files = get_time_control_ascii_filename_candidates(self.testscene, tmpdirname)

        self.assertEqual(len(ascii_files), 0)

    def test_get_time_control_ascii_filename_candidates_orbit_off_by1(self):

        with tempfile.TemporaryDirectory() as tmpdirname:

            self.file1 = os.path.join(tmpdirname, self.filename1)
            with open(self.file1, 'w') as _:
                pass

            self.testscene['orbit_number'] = self.testscene['orbit_number'] + 1
            ascii_files = get_time_control_ascii_filename_candidates(self.testscene, tmpdirname)

        self.assertEqual(len(ascii_files), 1)
        self.assertTrue(os.path.basename(ascii_files[0]) ==
                        'S_NWC_timectrl_metopb_46878_20210930T0946289Z_20210930T1001458Z.txt')

    def test_get_time_control_ascii_filename_candidates_modis_zero_orbit(self):

        with tempfile.TemporaryDirectory() as tmpdirname:

            self.file1 = os.path.join(tmpdirname, self.filename1_modis)
            with open(self.file1, 'w') as _:
                pass

            ascii_files = get_time_control_ascii_filename_candidates(self.modis_scene, tmpdirname)

        self.assertEqual(len(ascii_files), 1)
        self.assertTrue(os.path.basename(ascii_files[0]) ==
                        'S_NWC_timectrl_eos2_00000_20220209T2210110Z_20220209T2212284Z.txt')


class TestProductStatisticsFiles(unittest.TestCase):
    """Testing locating the product statistics XML files."""

    def setUp(self):
        self.testscene = {'platform_name': 'Metop-B', 'orbit_number': 46878, 'satday':
                          '20210930', 'sathour': '0946',
                          'starttime': datetime(2021, 9, 30, 9, 46, 24),
                          'endtime': datetime(2021, 9, 30, 10, 1, 43)}

        self.filename1 = 'S_NWC_CTTH_metopb_46878_20210930T0946289Z_20210930T1001458Z_statistics.xml'
        self.filename2 = 'S_NWC_CTTH_metopb_46878_20210930T0949289Z_20210930T1001459Z_statistics.xml'
        self.filename3 = 'S_NWC_CTTH_metopb_46878_20210930T0947019Z_20210930T1001458Z_statistics.xml'

        self.pattern = ('S_NWC_{product:s}_{satellite:s}_{orbit:s}_{starttime:%Y%m%dT%H%M}{seconds1:s}' +
                        '_{endtime:%Y%m%dT%H%M}{seconds2}_statistics.xml')

    def test_get_product_statistics_files_fewseconds_off(self):
        """Test get the list of product statistics files."""
        # get_product_statistics_files
        with tempfile.TemporaryDirectory() as tmpdirname:

            file1 = os.path.join(tmpdirname, self.filename1)
            with open(file1, 'w') as _:
                pass
            file2 = os.path.join(tmpdirname, self.filename2)
            with open(file2, 'w') as _:
                pass

            xmlfiles = get_product_statistics_files(tmpdirname, self.testscene, self.pattern, 1)
            self.assertTrue(len(xmlfiles) == 1)

            filename = os.path.basename(xmlfiles[0])
            self.assertEqual(filename, 'S_NWC_CTTH_metopb_46878_20210930T0946289Z_20210930T1001458Z_statistics.xml')

        with tempfile.TemporaryDirectory() as tmpdirname:

            file1 = os.path.join(tmpdirname, self.filename1)
            with open(file1, 'w') as _:
                pass
            file2 = os.path.join(tmpdirname, self.filename2)
            with open(file2, 'w') as _:
                pass

            xmlfiles = get_product_statistics_files(tmpdirname, self.testscene, self.pattern, 0)
            self.assertTrue(len(xmlfiles) == 1)

            filename = os.path.basename(xmlfiles[0])
            self.assertEqual(filename, 'S_NWC_CTTH_metopb_46878_20210930T0946289Z_20210930T1001458Z_statistics.xml')

    def test_get_product_statistics_files_oneminute_off(self):
        """Test get the list of product statistics files."""
        # get_product_statistics_files
        with tempfile.TemporaryDirectory() as tmpdirname:

            file3 = os.path.join(tmpdirname, self.filename3)
            with open(file3, 'w') as _:
                pass

            xmlfiles = get_product_statistics_files(tmpdirname, self.testscene, self.pattern, 0)
            self.assertTrue(len(xmlfiles) == 0)

        with tempfile.TemporaryDirectory() as tmpdirname:

            file3 = os.path.join(tmpdirname, self.filename3)
            with open(file3, 'w') as _:
                pass

            xmlfiles = get_product_statistics_files(tmpdirname, self.testscene, self.pattern, 1)
            self.assertTrue(len(xmlfiles) == 1)

            filename = os.path.basename(xmlfiles[0])
            self.assertEqual(filename, 'S_NWC_CTTH_metopb_46878_20210930T0947019Z_20210930T1001458Z_statistics.xml')


if __name__ == "__main__":
    pass
