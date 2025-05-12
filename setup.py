#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2013 - 2022 Pytroll Community

# Author(s):

#   Martin Raspaud <martin.raspaud@smhi.se>
#   Adam Dybbroe <adam.dybbroe@smhi.se>

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

"""Setup for pps-runner."""
from setuptools import find_packages, setup

try:
    # HACK: https://github.com/pypa/setuptools_scm/issues/190#issuecomment-351181286
    # Stop setuptools_scm from including all repository files
    import setuptools_scm.integration
    setuptools_scm.integration.find_files = lambda _: []
except ImportError:
    pass

description = 'Pytroll runner for PPS'

try:
    with open('./README', 'r') as fd:
        long_description = fd.read()
except IOError:
    long_description = ''

NAME = "nwcsafpps_runner"

setup(name=NAME,
      description=description,
      author='Adam Dybroe',
      author_email='adam.dybroe@smhi.se',
      classifiers=["Development Status :: 3 - Alpha",
                   "Intended Audience :: Science/Research",
                   "License :: OSI Approved :: GNU General Public License v3 " +
                   "or later (GPLv3+)",
                   "Operating System :: OS Independent",
                   "Programming Language :: Python",
                   "Topic :: Scientific/Engineering"],
      url="https://github.com/pytroll/pytroll-pps-runner",
      long_description=long_description,
      license='GPLv3',
      packages=find_packages(),
      scripts=['bin/pps_runner.py',
               'bin/run_nwp_preparation.py',
               'bin/pps_l1c_collector.py',
               'bin/level1c_runner.py', ],
      data_files=[],
      install_requires=['posttroll', 'trollsift', 'pygrib', 'level1c4pps'],
      python_requires='>=3.6',
      zip_safe=False,
      setup_requires=['setuptools_scm', 'setuptools_scm_git_archive'],
      use_scm_version=True
      )
