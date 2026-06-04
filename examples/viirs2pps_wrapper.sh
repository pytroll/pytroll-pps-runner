#!/bin/bash
lvl1_files=$@
viirs2pps.py -o /san1/polar_in/lvl1c $lvl1_files
