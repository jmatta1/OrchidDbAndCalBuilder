#!/usr/bin/python
"""This takes the folder that data was processed by ORCHID reader, dumps the
csv files into the appropriate databases and determines reactor cycle (so that
the appropriate calibration lines can be chosen and used)"""
import sys
from odbblib import readrawdata as rrd
from odbblib import databaseops as dbops


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
    batch_location = sys.argv[1]
    print "Setting batch datebase path to:", batch_db_path
    print "Setting batch location to:", batch_location
    batch_data = rrd.read_batch_data(batch_location)
    for key in batch_data:
        print "%20s:"%key, batch_data[key]
    dbops.add_batch_data(batch_data, batch_db_path)
    #run_data = rrd.read_run_data(batch_location)


USAGE_INFO = """Usage:
    {0:s} <batch_info_file> [batch_database_path]"""


if __name__ == "__main__":
    main()
