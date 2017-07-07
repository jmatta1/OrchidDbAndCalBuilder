"""Contains the functions that read the raw csv files in the batch data"""

import os
import datetime as dt

PATH_STR = "/data1/prospect/ProcessedData/OrchidAnalysis/TimeSeries_2017"

def read_batch_data(fname):
    """Reads the batch information csv

    Parameters
    batch_location : str
        The directory containing the batch data

    Returns
    batch_data : dict
        Dictionary containing all the values in the raw batch data
    """
    infile = open(fname)
    # skip the header line
    _ = infile.readline()
    data = [x.strip() for x in infile.readline().strip().split(",")]
    run_data = {}
    _, run_data["BatchName"] = os.path.split(os.path.split(fname)[0])
    run_data["DetCount"] = int(data[0])
    run_data["ArrayX"] = float(data[1])
    run_data["ArrayY"] = float(data[2])
    run_data["IntTime"] = float(data[3])
    if PATH_STR in data[4]:
        data[4] = "/home/jmatta1/test_data" + data[4][len(PATH_STR):]
    run_data["RootFileLocation"] = data[4]
    if PATH_STR in data[5]:
        data[5] = "/home/jmatta1/test_data" + data[5][len(PATH_STR):]
    run_data["RunDataLocation"] = data[5]
    if PATH_STR in data[6]:
        data[6] = "/home/jmatta1/test_data" + data[6][len(PATH_STR):]
    run_data["DetDataLocation"] = data[6]
    run_data["FirstBufferSkipped"] = False if data[7]=="No" else True
    run_data["TreeGenerated"] = False if data[8]=="No" else True
    run_data["TreeFileLocation"] = ""
    if run_data["TreeGenerated"]:
        run_data["TreeFileLocation"] = data[9]
    run_data["StartEpochMicroSec"] = int(data[10])
    run_data["StartDateTime"] = dt.datetime.strptime(data[11],
                                                     "%Y-%b-%d %H:%M:%S.%f")
    run_data["StopEpochMicroSec"] = int(data[12])
    run_data["StopDateTime"] = dt.datetime.strptime(data[13],
                                                    "%Y-%b-%d %H:%M:%S.%f")
    run_data["RunCount"] = int(data[14])
    test_year = "{0:d}".format(run_data["StartDateTime"].year)
    if not test_year in run_data["BatchName"][-4:]:
        run_data["BatchName"] += "_{0:s}".format(test_year)
    return run_data


def read_run_data(fname, det_count):
    """Reads the run information csv

    Parameters
    batch_location : str
        The directory containing the batch data

    Returns
    run_data : dict
        Dictionary containing, for each value in the raw run data, a list of
        that value for every run
    """
    infile = open(fname)
    # skip the header line
    _ = infile.readline()
    for line in infile:
        data = [x.strip() for x in line.strip().split(',')]


def read_det_data(fname):
    """Reads the det information csv

    Parameters
    batch_location : str
        The directory containing the batch data

    Returns
    det_data : dict
        Dictionary containing, for each value in the det data, a list of
        that value for detector
    """
    infile = open(fname)
    # skip the header line
    _ = infile.readline()
