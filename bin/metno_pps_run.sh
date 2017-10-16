#!/bin/bash

# $1 = Satellite
# $2 = Orbit number
# $3 = yyyymmdd
# $4 = hhmm
# $5 = lvl1_dir

#. /local_disk/opt/acpg/cfg/.profile_pps
#. /software/polsatproc/pps-v2014/run-acpg/cfg/.profile_pps
#source /vol/software/polsatproc/pps-v2014/source_me_MET-20141015
#source /home/ubuntu/pytroll/etc/source_me_MET-pps-v2014-xenial

echo $1
echo $2
echo $3
echo $4
echo $5

if [ "$5" == "L-BAND" ]; then
    source_file=/home/ubuntu/pytroll/etc/source_me_MET-pps-v2014-xenial-l
elif [ "$5" == "XL-BAND" ]; then
    source_file=/home/ubuntu/pytroll/etc/source_me_MET-pps-v2014-xenial-xl
else
    source_file=/home/ubuntu/pytroll/etc/source_me_MET-pps-v2014-xenial-$5
fi

source $source_file

env

#if [ "x$3" == "x0" ] && [ "x$4" == "x0" ]; then
#    PPS_OPTIONS="-p $1 $2"
#else
#    PPS_OPTIONS="-p $1 $2 --satday $3 --sathour $4"
#fi

#python /software/polsatproc/pps-v2014/run-acpg/scr/ppsMakeAvhrr.py $PPS_OPTIONS
#python /software/polsatproc/pps-v2014/run-acpg/scr/ppsMakePhysiography.py $PPS_OPTIONS
#python /software/polsatproc/pps-v2014/run-acpg/scr/ppsMakeNwp.py $PPS_OPTIONS
#python /software/polsatproc/pps-v2014/run-acpg/scr/ppsCmaskPrepare.py $PPS_OPTIONS
#python /software/polsatproc/pps-v2014/run-acpg/scr/ppsCmask.py $PPS_OPTIONS
#python /software/polsatproc/pps-v2014/run-acpg/scr/ppsCtype.py $PPS_OPTIONS

#python /software/polsatproc/pps-v2014/run-acpg/scr/ppsPrecipPrepare.py $PPS_OPTIONS

if [ "x$6" != "x" ]; then
    SM_AAPP_DATA_DIR=$6
    export SM_AAPP_DATA_DIR
    echo "Level 1 dir = $SM_AAPP_DATA_DIR"
fi

if [ "x$3" == "x0" ] && [ "x$4" == "x0" ]; then
    echo "USING: python /opt/acpg/scr/ppsRunAllParallel.py -p $1 $2 --cpp 0 --precip 0"
    python /opt/acpg/scr/ppsRunAllParallel.py -p $1 $2 --cpp 0 --precip 0
else
    echo "USING: python /opt/acpg/scr/ppsRunAllParallel.py -p $1 $2 --satday $3 --sathour $4 --cpp 0 --precip 0"
    python /opt/acpg/scr/ppsRunAllParallel.py -p $1 $2 --satday $3 --sathour $4 --cpp 0 --precip 0
fi
