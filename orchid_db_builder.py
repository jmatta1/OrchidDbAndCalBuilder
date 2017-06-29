#!/usr/bin/python
"""This takes the folder that data was processed by ORCHID reader, dumps the
csv files into the appropriate databases and determines reactor cycle (so that
the appropriate calibration lines can be chosen and used)"""
import sqlite3 as sql
import sys


# BATCH_DB_LOCATION = "/data1/prospect/ProcessedData/OrchidAnalysis"
BATCH_DB_LOCATION = "/home/jmatta1/test_data"


def main():
    """This function is the main entry point for the program"""
    pass
    


if __name__ == "__main__":
    main()
