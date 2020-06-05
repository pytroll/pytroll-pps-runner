#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (c) 2020 pps2018_runner developers
#
# Author(s):
#
#   Erik Johansson <erik.johansson@smhi.se>
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
from nwcsafpps_runner.utils import get_outputfiles
import os


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
    d = tmp_path / "export"
    d.mkdir()

    #: Create test files
    def create_files(d, typ):
        #: These files should always be found
        f1 = d / "S_NWC_CMAPROB_noaa15_12345_19810305T0715000Z_19810305T0730000Z.{}".format(typ)
        f1.write_text("correct orbit and time")
        #: These files should be found if start time is not given
        f2 = d / "S_NWC_CMAPROB_noaa15_12345_19810305T0745000Z_19810305T0800000Z.{}".format(typ)
        f2.write_text("correct orbit and time within 90 min")
        #: These files should not be found although the start time is correct
        f3 = d / "S_NWC_CMAPROB_noaa15_54321_19810305T0715000Z_19810305T0730000Z.{}".format(typ)
        f3.write_text("wrong orbit and correct time")

    #: Test xml files without start time
    typ = "xml"
    create_files(d, typ)
    expected = [os.path.join(d, "S_NWC_CMAPROB_noaa15_12345_19810305T0715000Z_19810305T0730000Z.{}".format(typ)),
                os.path.join(d, "S_NWC_CMAPROB_noaa15_12345_19810305T0745000Z_19810305T0800000Z.{}".format(typ))]
    res = get_outputfiles(d, "noaa15", 12345, xml_output=True)
    assert len(res) == len(set(res))
    assert set(res) == set(expected)
    #: Test xml files with start time
    expected = [os.path.join(d, "S_NWC_CMAPROB_noaa15_12345_19810305T0715000Z_19810305T0730000Z.{}".format(typ))]
    res = get_outputfiles(d, "noaa15", 12345, st_time="19810305T0715", xml_output=True)
    assert len(res) == len(set(res))
    assert set(res) == set(expected)

    #: Test h5 files without start time
    typ = "h5"
    create_files(d, typ)
    expected = [os.path.join(d, "S_NWC_CMAPROB_noaa15_12345_19810305T0715000Z_19810305T0730000Z.{}".format(typ)),
                os.path.join(d, "S_NWC_CMAPROB_noaa15_12345_19810305T0745000Z_19810305T0800000Z.{}".format(typ))]
    res = get_outputfiles(d, "noaa15", 12345, h5_output=True)
    assert len(res) == len(set(res))
    assert set(res) == set(expected)
    #: Test h5 files with start time
    expected = [os.path.join(d, "S_NWC_CMAPROB_noaa15_12345_19810305T0715000Z_19810305T0730000Z.{}".format(typ))]
    res = get_outputfiles(d, "noaa15", 12345, st_time="19810305T0715", h5_output=True)
    assert len(res) == len(set(res))
    assert set(res) == set(expected)

    #: Test nc files without start time
    typ = "nc"
    create_files(d, typ)
    expected = [os.path.join(d, "S_NWC_CMAPROB_noaa15_12345_19810305T0715000Z_19810305T0730000Z.{}".format(typ)),
                os.path.join(d, "S_NWC_CMAPROB_noaa15_12345_19810305T0745000Z_19810305T0800000Z.{}".format(typ))]
    res = get_outputfiles(d, "noaa15", 12345, nc_output=True)
    assert len(res) == len(set(res))
    assert set(res) == set(expected)
    #: Test nc files with start time
    expected = [os.path.join(d, "S_NWC_CMAPROB_noaa15_12345_19810305T0715000Z_19810305T0730000Z.{}".format(typ))]
    res = get_outputfiles(d, "noaa15", 12345, st_time="19810305T0715", nc_output=True)
    assert len(res) == len(set(res))
    assert set(res) == set(expected)


if __name__ == "__main__":
    pass
