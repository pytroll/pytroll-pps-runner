#!/home/bin/python
import sys
import os
from ppsRunAll import pps_run_all_serial
from ppsCmaskProb import pps_cmask_prob

infile = sys.argv[1]
base_infile = os.path.basename(infile)
# Skip cmaprob to get CTTH as fast as possible
pps_run_all_serial(anglesfile=base_infile, no_cmaskprob_arg=True, no_cmaskprob=True)
# Run CMAPROB
pps_cmask_prob(anglesfile=base_infile)
