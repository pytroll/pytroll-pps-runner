#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2026 Pytroll Community

# Author(s):

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

"""Test the logger code."""
import pytest


TEST_PPS_LOG = """
version: 1
disable_existing_loggers: false
formatters:
  pytroll:
    format: '[%(asctime)s %(levelname)-8s %(name)s] %(message)s'
handlers:
  console:
    class: logging.StreamHandler
    level: DEBUG
    formatter: pytroll
    stream: ext://sys.stdout
loggers:
  posttroll:
    level: ERROR
    propagate: false
    handlers: [console, ]
root:
  level: DEBUG
  handlers: [console, ]
"""


@pytest.fixture
def fake_file(tmp_path):
    """Create directory with test files."""
    file_cfg = tmp_path / 'pps_log_config.yaml'
    file_h = open(file_cfg, 'w')
    file_h.write(TEST_PPS_LOG.replace("my_test_dir", str(tmp_path)))
    file_h.close()
    return str(file_cfg)


class TestLogger:
    """Test the logger."""

    def test_logger(self, fake_file):
        """Test the pps_collector_runner."""
        from nwcsafpps_runner.logger import setup_logging
        from argparse import Namespace
        myconfig_filename = fake_file
        cmd_args = Namespace(log_config=myconfig_filename)
        setup_logging(cmd_args)
