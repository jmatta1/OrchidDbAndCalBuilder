#!/usr/bin/python
"""This takes the folder that data was processed by ORCHID reader, dumps the
csv files into the appropriate databases and determines reactor cycle (so that
the appropriate calibration lines can be chosen and used)"""
import sys
import os
from odbblib import readrawdata as rrd
from odbblib import databaseops as dbops
from odbblib import input_sanitizer as ins


# BATCH_DB_LOCATION = "/data1/prospect/ProcessedData/OrchidAnalysis/batchDatabase.db"
BATCH_DB_LOCATION = "/home/jmatta1/test_data/batchDatabase.db"


def main():
    """This function is the main entry point for the program"""
    if not len(sys.argv) in [2,3]:
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
    # attempt to insert the batch data into the global batch database
    if not dbops.add_batch_data(batch_data, batch_db_path):
        print "Batch information already in database, choose an action"
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
    # read the detector metadata
    det_data = rrd.read_det_data(batch_data["DetDataLocation"])
    # read the run data
    run_data = rrd.read_run_data(batch_data["RunDataLocation"], det_data)
    # break the run data into more useful format
    run_info = [x[0] for x in run_data]
    det_run_data = [[x[ind] for x in run_data] for ind in
                    range(1, len(run_data[0]))]
    # generate the path for the run database
    base, _ = os.path.split(batch_data_location)
    run_db_path = os.path.join(base, "runDatabase.db")
    # attempt to put the data into the run database
    dbops.make_batch_database(run_db_path, det_data, run_info, det_run_data)


USAGE_INFO = """Usage:
    {0:s} <batch_info_file> [batch_database_path]"""


if __name__ == "__main__":
    main()
