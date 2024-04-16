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

"""Prepare NWP data for PPS."""

import logging
import os
import tempfile
import time
from configparser import NoOptionError
from datetime import datetime, timezone
from glob import glob

import pygrib  # @UnresolvedImport
from trollsift import Parser

from nwcsafpps_runner.config import load_config_from_file
from nwcsafpps_runner.utils import NwpPrepareError, run_command

LOG = logging.getLogger(__name__)


class NWPFileFamily(object):
    """Container for a nwp file family."""

    def __init__(self, cfg, filename):
        """Container for nwp-file object."""
        self.nhsf_file = filename
        self.nhsp_file = filename.replace(cfg["nhsf_path"], cfg["nhsp_path"]).replace(
            cfg["nhsf_prefix"], cfg["nhsp_prefix"])
        self.file_end = os.path.basename(filename).replace(cfg["nhsf_prefix"], "")
        self.tmp_filename = make_temp_filename(suffix="_" + self.file_end, dir=cfg["nwp_outdir"])
        self.tmp_result_filename = self.tmp_filename + "_tmp_result"
        self.tmp_result_filename_reduced = self.tmp_filename + "_tmp_result_reduced"
        out_name = cfg["nwp_output_prefix"] + self.file_end
        self.result_file = os.path.join(cfg["nwp_outdir"], out_name)
        self.forecast_step = None
        self.analysis_time = None
        self.timestamp = None
        self.nwp_lsmz_filename = cfg["nwp_static_surface"]
        self.nwp_req_filename = cfg["pps_nwp_requirements"]
        self.cfg = cfg
        self.set_time_info(filename, cfg)

    def set_time_info(self, filename, cfg):
        """Parse time info from a file."""
        try:
            parser = Parser(cfg["nhsf_file_name_sift"])
        except NoOptionError as noe:
            LOG.error("NoOptionError {}".format(noe))
        if not parser.validate(os.path.basename(self.nhsf_file)):
            LOG.error("Parser validate on filename: {} failed.".format(self.nhsf_file))
        res = parser.parse("{}".format(os.path.basename(self.nhsf_file)))
        if 'analysis_time' in res:
            if res['analysis_time'].year == 1900:
                res['analysis_time'] = res['analysis_time'].replace(year=datetime.now(timezone.utc).year)
            self.analysis_time = res['analysis_time'].replace(tzinfo=timezone.utc)
            self.timestamp = self.analysis_time.strftime("%Y%m%d%H%M")
        else:
            raise NwpPrepareError("Can not parse analysis_time in file name. Check config and filename timestamp")
        if 'forecast_step' in res:
            self.forecast_step = res['forecast_step']
        else:
            raise NwpPrepareError(
                'Failed parsing forecast_step in file name. Check config and filename timestamp.')


def prepare_config(config_file_name):
    """Get config for NWP processing."""
    LOG.debug("Prepare_nwp config file = %s", str(config_file_name))

    cfg = load_config_from_file(config_file_name)

    for parameter in ['nhsp_path', 'nhsp_prefix',
                      'nhsf_file_name_sift',
                      'pps_nwp_requirements',
                      'nwp_output_prefix',
                      'nhsf_path', 'nhsf_prefix']:
        if parameter not in cfg:
            LOG.exception('Parameter "{:s}" not set in config file: '.format(parameter))

    if not os.path.exists(cfg['nwp_static_surface']):
        LOG.error("Config parameter nwp_static_surface: {:s} does not exist."
                  "Can't prepare NWP data".format(cfg['nwp_static_surface']))
        raise IOError('Failed getting static land-sea mask and topography')
    return cfg


def remove_file(filename):
    """Remove a temporary file."""
    if os.path.exists(filename):
        LOG.info("Removing tmp file: %s.", filename)
        os.remove(filename)


def make_temp_filename(*args, **kwargs):
    """Make a temporary file."""
    tmp_filename_handle, tmp_filename = tempfile.mkstemp(*args, **kwargs)
    os.close(tmp_filename_handle)
    return tmp_filename


def update_nwp(starttime, nlengths, config_file_name):
    """Get config options and then prepare nwp."""
    LOG.info("Path to prepare_nwp config file = %s", config_file_name)
    cfg = prepare_config(config_file_name)
    return update_nwp_inner(starttime, nlengths, cfg)


def should_be_skipped(file_obj, starttime, nlengths):
    """Skip some files.

    Consider only analysis times newer than
    *starttime*. And consider only the forecast lead times in hours given by
    the list *nlengths* of integers. Never reprocess.

    """
    if file_obj.analysis_time < starttime:
        return True
    if file_obj.forecast_step not in nlengths:
        LOG.debug("Skip step. Forecast step and nlengths: {:s} {:s}".format(
            str(file_obj.forecast_step), str(nlengths)))
        return True
    if not os.path.exists(file_obj.nhsp_file):
        LOG.warning("Corresponding nhsp-file not there: {:s}".format(file_obj.nhsp_file))
        return True
    if os.path.exists(file_obj.result_file):
        LOG.info("File: {:s} already there...".format(file_obj.result_file))
        return True
    return False


def get_files_to_process(cfg):
    """Get all nhsf files in nhsf directory."""
    filelist = glob(os.path.join(cfg["nhsf_path"], cfg["nhsf_prefix"] + "*"))
    if len(filelist) == 0:
        LOG.info("No input files! dir = {:s}".format(cfg["nhsf_path"]))
        return []
    LOG.debug('NHSF NWP files found = {:s}'.format(str(filelist)))
    return filelist


def create_nwp_file(file_obj):
    """Create a new nwp file."""
    LOG.info("Result and tmp files:\n\t {:s}\n\t {:s}\n\t {:s}\n\t {:s}".format(
        file_obj.result_file,
        file_obj.tmp_filename,
        file_obj.tmp_result_filename,
        file_obj.tmp_result_filename_reduced))

    # probably to make sure files are not written at the moment!
    cmd = "grib_copy -w gridType=regular_ll {:s} {:s}".format(file_obj.nhsp_file,
                                                              file_obj.tmp_filename)
    retv = run_command(cmd)
    LOG.debug("Returncode = " + str(retv))
    if retv != 0:
        LOG.error(
            "Failed doing the grib-copy! Will continue with the next file")
        return None

    cmd = "cat {:s} {:s} {:s} > {:s}".format(file_obj.tmp_filename,
                                             file_obj.nhsf_file,
                                             file_obj.nwp_lsmz_filename,
                                             file_obj.tmp_result_filename)
    LOG.debug("Merge data and add topography and land-sea mask:")
    LOG.debug("Command = " + str(cmd))
    _start = time.time()
    retv = os.system(cmd)
    _end = time.time()
    LOG.debug("os.system call took: %f seconds", _end - _start)
    LOG.debug("Returncode = " + str(retv))
    if retv != 0:
        LOG.warning("Failed generating nwp file {:} ...".format(file_obj.result_file))
        raise IOError("Failed merging grib data")
    nwp_file_ok = check_and_reduce_nwp_content(file_obj.tmp_result_filename,
                                               file_obj.tmp_result_filename_reduced,
                                               file_obj.nwp_req_filename)

    if nwp_file_ok is None:
        LOG.info('NWP file content could not be checked, use anyway.')
        os.rename(file_obj.tmp_result_filename, file_obj.result_file)
        LOG.debug("Renamed file {:s} to {:s}".format(file_obj.tmp_result_filename,
                                                     file_obj.result_file))
    elif nwp_file_ok:
        os.rename(file_obj.tmp_result_filename_reduced, file_obj.result_file)
        LOG.debug("Renamed file {:s} to {:s}".format(file_obj.tmp_result_filename_reduced,
                                                     file_obj.result_file))
        LOG.info('NWP file with reduced content has been created: {:s}'.format(
            file_obj.result_file))
    else:
        LOG.warning("Missing important fields. No nwp file ({:s}) created".format(
                    file_obj.result_file))
        return None
    return file_obj.result_file


def update_nwp_inner(starttime, nlengths, cfg):
    """Prepare NWP grib files for PPS.

    Consider only analysis times newer than
    *starttime*. And consider only the forecast lead times in hours given by
    the list *nlengths* of integers
    """
    LOG.info("Path to nhsf files: {:s}".format(cfg["nhsf_path"]))
    LOG.info("Path to nhsp files: {:s}".format(cfg["nhsp_path"]))
    LOG.info("nwp_output_prefix {:s}".format(cfg["nwp_output_prefix"]))
    ok_files = []
    for fname in get_files_to_process(cfg):
        file_obj = NWPFileFamily(cfg, fname)
        if should_be_skipped(file_obj, starttime, nlengths):
            remove_file(file_obj.tmp_filename)
            continue
        LOG.debug("Analysis time and start time: {:s} {:s}".format(str(file_obj.analysis_time),
                                                                   str(starttime)))
        LOG.info("timestamp, step: {:s} {:s}".format(file_obj.timestamp,
                                                     str(file_obj.forecast_step)))
        out_file = create_nwp_file(file_obj)
        remove_file(file_obj.tmp_result_filename_reduced)
        remove_file(file_obj.tmp_result_filename)
        remove_file(file_obj.tmp_filename)
        if out_file is not None:
            ok_files.append(out_file)
    return ok_files, cfg.get("publish_topic", None)


def get_mandatory_and_all_fields(lines):
    """Get info requirement file. Mandatory lines starts with M.

    M 129 Geopotential 100 isobaricInhPa
    O 129 Geopotential 350 isobaricInhPa

    Returns:
       ["129 100 isobaricInhPa"],  ["129 100 isobaricInhPa", "129 350 isobaricInhPa"]

    """
    mandatory_lines = [ll.strip('M ').strip('\n') for ll in lines if str(ll).startswith('M')]
    mandatory_fields = [" ".join([line.split(" ")[ind] for ind in [0, -2, -1]]) for line in mandatory_lines]
    all_fields = [" ".join([line.strip('\n').split(" ")[ind] for ind in [1, -2, -1]]) for line in lines]
    return mandatory_fields, all_fields


def get_nwp_requirement(nwp_req_filename):
    """Read the new requirement file. Return list with mandatory and wanted fields."""
    try:
        with open(nwp_req_filename, 'r') as fpt:
            lines = fpt.readlines()
    except (IOError, FileNotFoundError):
        LOG.exception(
            "Failed reading nwp-requirements file: %s", nwp_req_filename)
        LOG.warning("Cannot check if NWP files is ok!")
        return None, None
    return get_mandatory_and_all_fields(lines)


def check_nwp_requirement(grb_entries, mandatory_fields, result_file):
    """Check nwp file all mandatory enteries should be present."""
    grb_entries.sort()
    for item in mandatory_fields:
        if item not in grb_entries:
            LOG.warning("Mandatory field missing in NWP file: %s", str(item))
            return False
    LOG.info("NWP file has all required fields for PPS: %s", result_file)
    return True


def check_and_reduce_nwp_content(gribfile, result_file, nwp_req_filename):
    """Check the content of the NWP file. Create a reduced file."""
    LOG.info("Get nwp requirements.")
    mandatory_fields, all_fields = get_nwp_requirement(nwp_req_filename)
    if mandatory_fields is None:
        return None

    LOG.info("Write fields specified in %s to file: %s", nwp_req_filename, result_file)
    grbout = open(result_file, 'wb')
    with pygrib.open(gribfile) as grbs:
        grb_entries = []
        for grb in grbs:
            field_id = ("%s %s %s" % (grb['paramId'],
                                      grb['level'],
                                      grb['typeOfLevel']))
            if field_id in all_fields:
                # Keep fields from nwp_req_filename
                grb_entries.append(field_id)
                msg = grb.tostring()
                grbout.write(msg)
    grbout.close()
    LOG.info("Check fields in file: %s", result_file)
    return check_nwp_requirement(grb_entries, mandatory_fields, result_file)


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
    now = datetime.now(tz=timezone.utc)
    update_nwp(now - timedelta(days=2), [9])
