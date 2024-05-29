#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2018 - 2022 Pytroll Developers

# Author(s):

#   Nina.Hakansson <Firstname.Lastname at smhi.se>

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

import argparse
from nwcsafpps_runner.utils import  process_timectrl_xml_from_pps_result_file_with_extension

if __name__ == "__main__":
    """From a PPS-file create the time-control xml file."""
    parser = argparse.ArgumentParser(
        description=('Script to produce a timectrl xml file from a PPS-file'))
    parser.add_argument('-f', '--pps_file', required=True, type=str, help='A PPS file, for which timectrl in xml format is needed')
    parser.add_argument('--extension', required=False, default="",
                        help="Output filename will be {pps-timectrl-file-name-prefix}{extension}.xml")
    parser.add_argument('--pps_statistics_dir', type=str, nargs='?',
                        required=False, default=None,
                        help="Output directory where to store the xml file")
    options = parser.parse_args()
    process_timectrl_xml_from_pps_result_file_with_extension(options.pps_file, options.pps_statistics_dir, options.extension)
