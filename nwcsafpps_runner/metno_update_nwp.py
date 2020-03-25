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
from nwcsafpps_runner.helper_functions import run_command, run_shell_command

import numpy as np

from eccodes import *

LOG = logging.getLogger(__name__)

def product(*args, **kwds):
    # product('ABCD', 'xy') --> Ax Ay Bx By Cx Cy Dx Dy
    # product(range(2), repeat=3) --> 000 001 010 011 100 101 110 111
    pools = list(map(tuple, args)) * kwds.get('repeat', 1)
    result = [[]]
    for pool in pools:
        result = [x + [y] for x in result for y in pool]
    for prod in result:
        yield tuple(prod)

#def copy_needed_field(gid, mgid):
def copy_needed_field(gid, fout):
    #LOG.debug("Need this")
    nx = codes_get(gid, 'Ni')
    ny = codes_get(gid, 'Nj')
    first_lat = codes_get(gid, 'latitudeOfFirstGridPointInDegrees')
    first_lon = codes_get(gid, 'longitudeOfFirstGridPointInDegrees')
    north_south_step = codes_get(gid, 'jDirectionIncrementInDegrees')
    east_west_step = codes_get(gid, 'iDirectionIncrementInDegrees')

    try:
        parameter = codes_get(gid, 'indicatorOfParameter')
    except:
        parameter = codes_get(gid, 'paramId')

    level = codes_get(gid, 'level')

    filter_north = 0

    new_ny = int((first_lat - filter_north)/north_south_step) + 1
    #print "new_ny: {}".format(new_ny)

    #print("Start reading values...")
    values = codes_get_values(gid)
    #print("End reading values...")
    values_r = np.reshape(values,(ny,nx))

    new_values = values_r[:new_ny,:]

    clone_id = codes_clone(gid)

    codes_set(clone_id, 'latitudeOfLastGridPointInDegrees', (filter_north))
    codes_set(clone_id, 'Nj', new_ny)

    codes_set_values(clone_id, new_values.flatten())

    #codes_grib_multi_append(clone_id, 0, mgid)
    codes_write(clone_id, fout)
    codes_release(clone_id)
    return

def update_nwp(params):
    LOG.info("METNO update nwp")

    result_files = dict()
    tempfile.tempdir = params['options']['nwp_outdir']

    ecmwf_path = params['options']['ecmwf_path']
    if not os.path.exists(ecmwf_path):
        ecmwf_path = ecmwf_path.replace("storeB","storeA")
        LOG.warning("Need to replace storeB with storeA for ecmwf_path: {}".format(str(ecmwf_path)))

    filelist = glob(os.path.join(ecmwf_path,  params['options']['ecmwf_prefix'] + "*"))

    if len(filelist) == 0:
        LOG.info("Found no input files! dir = " + str(os.path.join(ecmwf_path,  params['options']['ecmwf_prefix'] + "*")))
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
            step = "{:03d}H{:02d}M".format(int(step_delta.days*24 + step_delta.seconds/3600),0)
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
        output_parameters['step_hour'] = int(step_delta.days*24 + step_delta.seconds/3600)
        output_parameters['step_min'] = 0
        try:
            if not os.path.exists(params['options']['nwp_outdir']):
                os.makedirs(params['options']['nwp_outdir'])
        except OSError as e:
            LOG.error("Failed to create directory: %s", e)
        result_file = ""
        try:
            result_file = os.path.join(params['options']['nwp_outdir'], compose(params['options']['nwp_output'],output_parameters))
            _result_file = os.path.join(params['options']['nwp_outdir'], compose("."+params['options']['nwp_output'],output_parameters))
            _result_file_lock = os.path.join(params['options']['nwp_outdir'], compose("."+params['options']['nwp_output']+".lock",output_parameters))
        except Exception as e:
            LOG.error("Joining outdir with output for nwp failed with: {}".format(e))

        LOG.info("Result file: {}".format(result_file))
        if os.path.exists(result_file):
            LOG.info("File: " + str(result_file) + " already there...")
            continue

        import fcntl
        import errno
        import time
        rfl = open(_result_file_lock,'w+')
        #do some locking
        while True:
            try:
                fcntl.flock(rfl, fcntl.LOCK_EX|fcntl.LOCK_NB)
                LOG.debug("1Got lock for NWP outfile: {}".format(result_file))
                break;
            except IOError as e:
                if e.errno != errno.EAGAIN:
                    raise
                else:
                    LOG.debug("Waiting for lock ... {}".format(result_file))
                    time.sleep(1)
            
        if os.path.exists(result_file):
            LOG.info("File: " + str(result_file) + " already there...")
            #Need to release the lock
            fcntl.flock(rfl, fcntl.LOCK_UN)
            rfl.close()
            continue

        #Need to set up temporary file to copy grib fields to
        #If ram is available through /run/shm, use this, else use /tmp
        if os.path.exists("/run/shm"):
            __tmpfile = "/run/shm/__tmp"
        else:
            __tmpfile = "/tmp/__tmp"

        #mgid = codes_grib_multi_new()
        #codes_grib_multi_support_on()

        #Some parameters can be found from the first name, and some from paramID
        #Need to check the second of the first one is not found
        parameter_name_list = ["indicatorOfParameter","paramId"]

        fout = open(_result_file, 'wb')
        try:

            #Do the static fields
            #Note: field not in the filename variable, but a configured filename for static fields
            static_filename = params['options']['ecmwf_static_surface']
            #print("Handeling static file: %s", static_filename)
            if not os.path.exists(static_filename):
                static_filename = static_filename.replace("storeB","storeA")
                LOG.warning("Need to replace storeB with storeA")

            index_vals = []
            index_keys = ['paramId', 'level']
            LOG.debug("Start building index")
            LOG.debug("Handeling file: %s", filename)
            iid = codes_index_new_from_file(filename, index_keys)
            filename_n1s = filename.replace('N2D','N1S')
            LOG.debug("Add to index %s", filename_n1s)
            codes_index_add_file(iid, filename_n1s)
            LOG.debug("Add to index %s", static_filename)
            codes_index_add_file(iid, static_filename)
            LOG.debug("Done index")
            for key in index_keys:
                #print("size: ", key, codes_index_get_size(iid, key))
                key_vals = codes_index_get(iid, key)
                key_vals = tuple(x for x in key_vals if x != 'undef')
                #print(key_vals)
                #print(" ".join(key_vals))
                index_vals.append(key_vals)

            for prod in product(*index_vals):
                #print('All products: ', end='')
                for i in range(len(index_keys)):
                    #print('Range:', index_keys[i], prod[i])
                    #print("{} {}, ".format(index_keys[i], prod[i]), end='')
                    codes_index_select(iid, index_keys[i], prod[i])
                #print()
                while 1:
                    gid = codes_new_from_index(iid)
                    if gid is None:
                        break
                    #print(" ".join(["%s=%s" % (key, codes_get(gid, key))
                    #                for key in index_keys]))
                    param = codes_get(gid, index_keys[0])
                    #print("Doing param:",param)
                    parameters = [172, 129, 235, 167, 168, 137, 130, 131, 132, 133, 134, 157]
                    if param in parameters:
                        LOG.debug("Doing param: %d",param)
                        #copy_needed_field(gid, mgid)
                        copy_needed_field(gid, fout)

                    codes_release(gid)
            codes_index_release(iid)

            #fout = open(_result_file, 'wb')
            #codes_grib_multi_write(mgid, fout)
            #codes_grib_multi_release(mgid)
 
            fout.close()
            os.rename(_result_file, result_file)
        except WrongLengthError as wle:
            LOG.error("Something wrong with the data: %s", wle)
            raise

        #In the end release the lock
        fcntl.flock(rfl, fcntl.LOCK_UN)
        rfl.close()
        os.remove(_result_file_lock)
    return

