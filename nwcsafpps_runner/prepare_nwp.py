#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2015 - 2021 Pytroll

# Author(s):

#   Adam.Dybbroe <adam.dybbroe@smhi.se>

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

"""Prepare NWP data for PPS
"""

from glob import glob
import os
from datetime import datetime
import time
import tempfile
from trollsift import Parser
import pygrib  # @UnresolvedImport
from six.moves.configparser import NoOptionError

from nwcsafpps_runner.config import get_config
from nwcsafpps_runner.config import CONFIG_FILE
from nwcsafpps_runner.config import CONFIG_PATH  # @UnresolvedImport
from nwcsafpps_runner.utils import run_command
from nwcsafpps_runner.utils import NwpPrepareError

import logging
LOG = logging.getLogger(__name__)


LOG.debug("Path to prepare_nwp config file = %s", str(CONFIG_PATH))
LOG.debug("Prepare_nwp config file = %s", str(CONFIG_FILE))
OPTIONS = get_config(CONFIG_FILE)

try:
    nhsp_path = OPTIONS['nhsp_path']
except KeyError:
    LOG.exception('Parameter not set in config file: ' + 'nhsp_path')
try:
    nhsp_prefix = OPTIONS['nhsp_prefix']
except KeyError:
    LOG.exception('Parameter not set in config file: ' + 'nhsp_prefix')

nhsf_file_name_sift = OPTIONS.get('nhsf_file_name_sift')

nhsf_path = OPTIONS.get('nhsf_path', None)
nhsf_prefix = OPTIONS.get('nhsf_prefix', None)
nwp_outdir = OPTIONS.get('nwp_outdir', None)
nwp_lsmz_filename = OPTIONS.get('nwp_static_surface', None)
nwp_output_prefix = OPTIONS.get('nwp_output_prefix', None)
nwp_req_filename = OPTIONS.get('pps_nwp_requirements', None)


def logreader(stream, log_func):
    while True:
        s = stream.readline()
        if not s:
            break
        log_func(s.strip())
    stream.close()


def make_temp_filename(*args, **kwargs):
    tmp_filename_handle, tmp_filename = tempfile.mkstemp(*args, **kwargs)
    os.close(tmp_filename_handle)
    return tmp_filename


def update_nwp(starttime, nlengths):
    """Prepare NWP grib files for PPS. Consider only analysis times newer than
    *starttime*. And consider only the forecast lead times in hours given by
    the list *nlengths* of integers

    """

    LOG.info("Path to prepare_nwp config file = %s", str(CONFIG_PATH))
    LOG.info("Prepare_nwp config file = %s", str(CONFIG_FILE))
    LOG.info("Path to nhsf files: %s", str(nhsf_path))
    LOG.info("Path to nhsp files: %s", str(nhsp_path))

    tempfile.tempdir = nwp_outdir
    filelist = glob(os.path.join(nhsf_path, nhsf_prefix + "*"))
    if len(filelist) == 0:
        LOG.info("No input files! dir = %s", str(nhsf_path))
        return

    LOG.debug('NHSF NWP files found = %s', str(filelist))
    nfiles_error = 0
    for filename in filelist:
        if nhsf_file_name_sift is None:
            raise NwpPrepareError()

        try:
            parser = Parser(nhsf_file_name_sift)
        except NoOptionError as noe:
            LOG.error("NoOptionError {}".format(noe))
            continue
        if not parser.validate(os.path.basename(filename)):
            LOG.error("Parser validate on filename: {} failed.".format(filename))
            continue

        res = parser.parse("{}".format(os.path.basename(filename)))
        if 'analysis_time' in res:
            if res['analysis_time'].year == 1900:
                res['analysis_time'] = res['analysis_time'].replace(year=datetime.utcnow().year)

            analysis_time = res['analysis_time']
            timestamp = analysis_time.strftime("%Y%m%d%H%M")
        else:
            raise NwpPrepareError("Can not parse analysis_time in file name. Check config and filename timestamp")

        if 'forecast_time' in res:
            if res['forecast_time'].year == 1900:
                res['forecast_time'] = res['forecast_time'].replace(year=datetime.utcnow().year)
            forecast_time = res['forecast_time']
            forecast_step = forecast_time - analysis_time
            forecast_step = "{:03d}H{:02d}M".format(forecast_step.days*24 + forecast_step.seconds/3600, 0)
            timeinfo = "{:s}{:s}{:s}".format(analysis_time.strftime(
                "%m%d%H%M"), forecast_time.strftime("%m%d%H%M"), res['end'])
        else:
            # This needs to be done more solid using the sift pattern! FIXME!
            timeinfo = filename.rsplit("_", 1)[-1]
            # Forecast step in hours:
            if 'forecast_step' in res:
                forecast_step = res['forecast_step']
            else:
                raise NwpPrepareError(
                    'Failed parsing forecast_step in file name. Check config and filename timestamp.')

        if analysis_time < starttime:
            continue
        if forecast_step not in nlengths:
            LOG.debug("Skip step. Forecast step and nlengths: %s %s", str(forecast_step), str(nlengths))
            continue

        LOG.debug("Analysis time and start time: %s %s", str(analysis_time), str(starttime))
        LOG.info("timestamp, step: %s %s", str(timestamp), str(forecast_step))
        result_file = os.path.join(
            nwp_outdir, nwp_output_prefix + timestamp + "+" + '%.3dH00M' % forecast_step)
        if os.path.exists(result_file):
            LOG.info("File: " + str(result_file) + " already there...")
            continue

        tmp_filename = make_temp_filename(suffix="_" + timestamp + "+" +
                                          '%.3dH00M' % forecast_step, dir=nwp_outdir)

        LOG.info("result and tmp files: " + str(result_file) + " " + str(tmp_filename))
        nhsp_file = os.path.join(nhsp_path, nhsp_prefix + timeinfo)
        if not os.path.exists(nhsp_file):
            LOG.warning("Corresponding nhsp-file not there: " + str(nhsp_file))
            continue

        cmd = ("grib_copy -w gridType=regular_ll " + nhsp_file + " " + tmp_filename)
        retv = run_command(cmd)
        LOG.debug("Returncode = " + str(retv))
        if retv != 0:
            LOG.error(
                "Failed doing the grib-copy! Will continue with the next file")
            nfiles_error = nfiles_error + 1
            if nfiles_error > len(filelist) / 2:
                LOG.error(
                    "More than half of the Grib files failed upon grib_copy!")
                raise IOError('Failed running grib_copy on many Grib files')

        if not os.path.exists(nwp_lsmz_filename):
            LOG.error("No static grib file with land-sea mask and " +
                      "topography available. Can't prepare NWP data")
            raise IOError('Failed getting static land-sea mask and topography')

        tmp_result_filename = make_temp_filename()
        cmd = ('cat ' + tmp_filename + " " +
               os.path.join(nhsf_path, nhsf_prefix + timeinfo) +
               " " + nwp_lsmz_filename + " > " + tmp_result_filename)
        LOG.debug("Add topography and land-sea mask to data:")
        LOG.debug("Command = " + str(cmd))
        _start = time.time()
        retv = os.system(cmd)
        _end = time.time()
        LOG.debug("os.system call took: %f seconds", _end - _start)
        LOG.debug("Returncode = " + str(retv))
        if retv != 0:
            LOG.warning("Failed generating nwp file %s ...", result_file)
            if os.path.exists(tmp_result_filename):
                os.remove(tmp_result_filename)
            raise IOError("Failed adding topography and land-sea " +
                          "mask data to grib file")

        if os.path.exists(tmp_filename):
            os.remove(tmp_filename)
        else:
            LOG.warning("tmp file %s gone! Cannot clean it...", tmp_filename)

        if check_nwp_content(tmp_result_filename):
            LOG.info('A check of the NWP file content has been attempted: %s',
                     result_file)
            _start = time.time()
            os.rename(tmp_result_filename, result_file)
            _end = time.time()
            LOG.debug("Rename file %s to %s: This took %f seconds",
                      tmp_result_filename, result_file, _end - _start)
        else:
            LOG.warning("Missing important fields. No nwp file %s written to disk",
                        result_file)
            if os.path.exists(tmp_result_filename):
                os.remove(tmp_result_filename)

    return


def check_nwp_content(gribfile):
    """Check the content of the NWP file. If all fields required for PPS is
    available, then return True

    """

    with pygrib.open(gribfile) as grbs:
        entries = []
        for grb in grbs:
            entries.append("%s %s %s %s" % (grb['paramId'],
                                            grb['name'],
                                            grb['level'],
                                            grb['typeOfLevel']))
        entries.sort()

    try:
        with open(nwp_req_filename, 'r') as fpt:
            lines = fpt.readlines()
    except IOError:
        LOG.exception(
            "Failed reading nwp-requirements file: %s", nwp_req_filename)
        LOG.warning("Cannot check if NWP files is ok!")
        return True

    srplines = [ll.strip('M ').strip('\n')
                for ll in lines if str(ll).startswith('M')]

    file_ok = True
    for item in srplines:
        if item not in entries:
            LOG.warning("Mandatory field missing in NWP file: %s", str(item))
            file_ok = False

    if file_ok:
        LOG.info("NWP file has all required fields for PPS: %s", gribfile)

    return file_ok


if __name__ == "__main__":

    #: Default time format
    _DEFAULT_TIME_FORMAT = '%Y-%m-%d %H:%M:%S'
    #: Default log format
    _DEFAULT_LOG_FORMAT = '[%(levelname)s: %(asctime)s : %(name)s] %(message)s'

    import sys
    handler = logging.StreamHandler(sys.stderr)
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter(fmt=_DEFAULT_LOG_FORMAT,
                                  datefmt=_DEFAULT_TIME_FORMAT)
    handler.setFormatter(formatter)
    logging.getLogger('').addHandler(handler)
    logging.getLogger('').setLevel(logging.DEBUG)
    logging.getLogger('posttroll').setLevel(logging.INFO)

    LOG = logging.getLogger('test_update_nwp')

    from datetime import timedelta
    now = datetime.utcnow()
    update_nwp(now - timedelta(days=1), [9])
