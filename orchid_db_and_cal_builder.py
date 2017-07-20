#!/usr/bin/python
"""This takes the folder that data was processed by ORCHID reader, dumps the
csv files into the appropriate databases and determines reactor cycle (so that
the appropriate calibration lines can be chosen and used)"""
import sys
import os
from odacblib import readrawdata as rrd
from odacblib import databaseops as dbops
from odacblib import input_sanitizer as ins
from odacblib import fuzzy_logic as fl
from odacblib import rootops as ro

# BATCH_DB_LOCATION = "/data1/prospect/ProcessedData/OrchidAnalysis/batchDatabase.db"
BATCH_DB_LOCATION = "/home/jmatta1/test_data/batchDatabase.db"


def main():
    """This function is the main entry point for the program"""
    if not len(sys.argv) in [2, 3]:
        print USAGE_INFO.format(sys.argv[0])
        sys.exit()
    batch_db_path = BATCH_DB_LOCATION
    if len(sys.argv) == 3:
        batch_db_path = sys.argv[2]
    batch_data_location = sys.argv[1]
    print "Setting batch database path to:", batch_db_path
    print "Setting batch location to:", batch_data_location
    # read the raw batch data
    batch_data = rrd.read_batch_data(batch_data_location)
    # generate the paths for various things
    base, _ = os.path.split(batch_data_location)
    batch_data["RunDbLoc"] = os.path.join(base, "runDatabase.db")
    batch_data["CalRootLoc"] = os.path.join(base, "cal_hists.root")
    batch_data["DecompRootLoc"] = os.path.join(base, "decomp_hists.root")
    handle_batch_data(batch_data, batch_db_path)
    # read the detector metadata
    det_data = rrd.read_det_data(batch_data["DetDataLocation"])
    # read the run data
    run_data = rrd.read_run_data(batch_data["RunDataLocation"], det_data)
    # break the run data into more useful format
    run_info = [x[0] for x in run_data]
    det_run_data = [[x[ind] for x in run_data] for ind in
                    range(1, len(run_data[0]))]
    # attempt to put the data into the run database
    dbops.make_batch_database(batch_data["RunDbLoc"], det_data, run_info, det_run_data)
    # figure out if we need to produce multiple sums
    summing_lists = fl.find_sum_ranges(run_info, det_run_data)
    # call the function to setup the calibration root file. it will determine
    # if re-summing is required or if we can simply use the existing sum
    # spectra that were generated
    ro.prep_calibration_file(summing_lists, batch_data["RootFileLocation"],
                             batch_data["CalRootLoc"], run_info)
    


def handle_batch_data(batch_data, batch_db_path):
    """Attempts to insert the data for the batch into the global batch database

    Parameters
    ----------
    batch_data : dict
        dictionary of batch information
    batch_db_path : str
        path to the global batch database
    """
    # attempt to insert the batch data into the global batch database
    if not dbops.add_batch_data(batch_data, batch_db_path):
        print "\nBatch information already in database, choose an action"
        print "    1 - Abort execution"
        print "    2 - Overwrite Batch Database Entry"
        print "    3 - Skip Writing Batch Database"
        ans = ins.get_int("Enter Option Number:", inclusive_lower_bound=1,
                          inclusive_upper_bound=3, default_value=1)
        if ans == 1:
            print "Aborting execution"
            sys.exit()
        elif ans == 2:
            dbops.overwrite_batch_data(batch_data, batch_db_path)
            print "Overwrote batch database entry"
        elif ans == 3:
            print "Skipping insertion of batch data into global batch database"
    else:
        print "Added batch information to global batch database"


USAGE_INFO = """Usage:
    {0:s} <batch_info_file> [batch_database_path]"""


if __name__ == "__main__":
    main()
