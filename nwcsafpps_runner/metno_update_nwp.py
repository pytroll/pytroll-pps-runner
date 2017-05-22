#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2016 Trygve Aspenes

# Author(s):

#   trygveas@met.no

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

"""Metno version of preparing the ECMWF nwp data for PPS
"""

import logging
import tempfile
from glob import glob
import os
from datetime import datetime
from helper_functions import run_command, run_shell_command

LOG = logging.getLogger(__name__)

def update_nwp(params):
    LOG.info("METNO update nwp")

    result_files = dict()
    tempfile.tempdir = params['options']['nwp_outdir']

    filelist = glob(os.path.join(params['options']['ecmwf_path'],  params['options']['ecmwf_prefix'] + "*"))

    if len(filelist) == 0:
        LOG.info("Found no input files! dir = " + str(os.path.join(params['options']['ecmwf_path'],  params['options']['ecmwf_prefix'] + "*")))
        return

    from trollsift import Parser, compose
    filelist.sort()
    for filename in filelist:
        if params['options']['ecmwf_file_name_sift'] != None:
            try:
                parser = Parser(params['options']['ecmwf_file_name_sift'])
            except NoOptionError as noe:
                LOG.error("NoOptionError {}".format(noe)) 
                continue
            if not parser.validate(os.path.basename(filename)):
                LOG.error("Parser validate on filename: {} failed.".format(filename))
                continue
            res = parser.parse("{}".format(os.path.basename(filename)))

            #This takes to long to complete.
            # if filename not in file_cache:
            #     cmd="grib_get -w count=1 -p dataDate {}".format(filename)
            #     run_shell_command(cmd, stdout_logfile='/tmp/dataDate')
            #     dataDate = open("/tmp/dataDate", 'r')
            #     dataDate_input = dataDate.read()
            #     dataDate.close()
            #     for dd in dataDate_input.splitlines():
            #         try:
            #             _dataDate = datetime.strptime(dd, "%Y%m%d")
            #         except Exception as e:
            #         LOG.error("Failed with :{}".format(e))
                    
            #     print "Data date is: {}".format(_dataDate)
            #     _file_cache[filename] = _dataDate
            #     file_cache.append(_file_cache)
            # else:
            #     print "already got datetime"

            time_now = datetime.utcnow()
            if 'analysis_time' in res:
                if res['analysis_time'].year == 1900:
                    #This is tricky. Filename is missing year in name
                    #Need to guess the year from a compination of year now
                    #and month now and month of the analysis time taken from the filename
                    #If the month now is 1(January) and the analysis month is 12,
                    #then the time has passed New Year, but the NWP analysis time is previous year.
                    if time_now.month == 1 and res['analysis_time'].month == 12:
                        analysis_year = time_now.year-1
                    else:
                        analysis_year = time_now.year

                    res['analysis_time'] = res['analysis_time'].replace( year = analysis_year)
            else:
                LOG.error("Can not parse analysis_time in file name. Check config and filename timestamp")

            if 'forecast_time' in res:
                if res['forecast_time'].year == 1900:
                    #See above for explanation
                    if res['analysis_time'].month == 12 and res['forecast_time'].month == 1:
                        forecast_year = res['analysis_time'].year+1
                    else:
                        forecast_year = res['analysis_time'].year
                        
                    res['forecast_time'] = res['forecast_time'].replace( year = forecast_year)
            else:
                LOG.error("Can not parse forecast_time in file name. Check config and filename timestamp")

            forecast_time = res['forecast_time']
            analysis_time = res['analysis_time']
            timestamp = analysis_time.strftime("%Y%m%d%H%M")
            step_delta = forecast_time - analysis_time
            step = "{:03d}H{:02d}M".format(step_delta.days*24 + step_delta.seconds/3600,0)
            timeinfo = "{:s}{:s}{:s}".format(analysis_time.strftime("%m%d%H%M"), forecast_time.strftime("%m%d%H%M"), res['end'])
        else:
            LOG.error("Not sift pattern given. Can not parse input NWP files")


        if analysis_time < params['starttime']:
            #LOG.debug("skip analysis time {} older than search time {}".format(analysis_time, params['starttime']))
            continue

        if int(step[:3]) not in params['nlengths']:
            #LOG.debug("Skip step {}, not in {}".format(int(step[:3]), params['nlengths']))
            continue

        output_parameters = {}
        output_parameters['analysis_time'] = analysis_time
        output_parameters['step_hour'] = step_delta.days*24 + step_delta.seconds/3600
        output_parameters['step_min'] = 0
        try:
            result_file = os.path.join(params['options']['nwp_outdir'], compose(params['options']['nwp_output'],output_parameters))
        except Exception as e:
            LOG.error("Joining outdir with output for nwp failed with: {}".format(e))

        LOG.info("Result file: {}".format(result_file))
        if os.path.exists(result_file):
            LOG.info("File: " + str(result_file) + " already there...")
            continue

        #Need to set up temporary file to copy grib fields to
        #If ram is available through /run/shm, use this, else use /tmp
        if os.path.exists("/run/shm"):
            __tmpfile = "/run/shm/__tmp"
        else:
            __tmpfile = "/tmp/__tmp"

        #Some parameters can be found from the first name, and some from paramID
        #Need to check the second of the first one is not found
        parameter_name_list = ["indicatorOfParameter","paramId"]

        #Do the static fields
        #Note: field not in the filename variable, but a configured filename for static fields
        parameters = [172, 129]
        level = None
        type_of_level = None
        static_filename = params['options']['ecmwf_static_surface']
        for par in parameters:
            for parameter_name in parameter_name_list:
                cmd = ("grib_copy -f -w {}:l={} {} {}".format(parameter_name, par, static_filename, __tmpfile))
                retv = run_command(cmd)
                #LOG.debug("Returncode = " + str(retv))
                if not retv and os.path.exists(__tmpfile):
                    #LOG.debug("append ...")
                    grib_filter = open("/tmp/rule-filter.append", 'w')
                    grib_filter.write("append;\n")
                    grib_filter.close()
                    cmd = ("grib_filter /tmp/rule-filter.append -o {} {}".format(result_file, __tmpfile))
                    retv = run_command(cmd)
                    os.remove(__tmpfile)
                    break
                else:
                    LOG.error("grib_copy failed and/or {} does not exist".format(__tmpfile))

        #Need to copy parameters for surface layers
        parameters = [235, 167, 168, 134, 137]
        level = 0
        type_of_level = 1
        for par in parameters:
            for parameter_name in parameter_name_list:
                cmd = ("grib_copy -f -w level:l={},indicatorOfTypeOfLevel:l={},{}:l={} {} {}".format(level, type_of_level, parameter_name, par, filename, __tmpfile))
                retv = run_command(cmd)
                #LOG.debug("Returncode = " + str(retv))
                if not retv and os.path.exists(__tmpfile):
                    #LOG.debug("append ...")
                    grib_filter = open("/tmp/rule-filter.append", 'w')
                    grib_filter.write("append;\n")
                    grib_filter.close()
                    cmd = ("grib_filter /tmp/rule-filter.append -o {} {}".format(result_file, __tmpfile))
                    retv = run_command(cmd)
                    os.remove(__tmpfile)
                    break
                else:
                    LOG.error("grib_copy failed and/or {} does not exist".format(__tmpfile))

        #Need to copy parameters for all levels
        parameters = [130, 131, 132, 133, 157, 129]
        level = None
        type_of_level = None
        for par in parameters:
            for parameter_name in parameter_name_list:
                cmd = ("grib_copy -f -w {}:l={} {} {}".format(parameter_name, par, filename, __tmpfile))
                retv = run_command(cmd)
                #LOG.debug("Returncode = " + str(retv))
                if not retv and os.path.exists(__tmpfile):
                    #LOG.debug("append ...")
                    grib_filter = open("/tmp/rule-filter.append", 'w')
                    grib_filter.write("append;\n")
                    grib_filter.close()
                    cmd = ("grib_filter /tmp/rule-filter.append -o {} {}".format(result_file, __tmpfile))
                    retv = run_command(cmd)
                    os.remove(__tmpfile)
                    if result_file not in result_files:
                        result_files[result_file] = filename
                    break
                else:
                    LOG.error("grib_copy failed and/or {} does not exist".format(__tmpfile))

        #if check_nwp_content(result_file):
        #    LOG.info('A check of the NWP file content has been attempted: %s',
        #             result_file)
        #    #os.rename(tmpresult, result_file)
        #else:
        #    LOG.warning("Missing important fields. No nwp file %s written to disk",
        #                result_file)

        #os.exit

    for result_file, filename in result_files.iteritems():
        LOG.debug("Need to add 235 to this {} {}".format(result_file, filename))
        filename_base = os.path.basename(filename)
        filename_base_time = filename_base[3:]
        filename_base_new = "N1S" + filename_base_time

        LOG.debug("Using new filename: {}".format(filename_base_new))
        par = 235
        parameter_name = "indicatorOfParameter"
        
        cmd = ("grib_copy -f -w {}:l={} {} {}".format(parameter_name, par, os.path.join(os.path.dirname(filename),filename_base_new), __tmpfile))
        retv = run_command(cmd)
        if not retv and os.path.exists(__tmpfile):
            grib_filter = open("/tmp/rule-filter.append", 'w')
            grib_filter.write("append;\n")
            grib_filter.close()
            cmd = ("grib_filter /tmp/rule-filter.append -o {} {}".format(result_file, __tmpfile))
            retv = run_command(cmd)
            os.remove(__tmpfile)
        else:
            LOG.error("grib_copy failed and/or {} does not exist".format(__tmpfile))

        
    return

