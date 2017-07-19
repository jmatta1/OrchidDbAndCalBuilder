"""Contains the functions that read the raw csv files in the batch data"""

import os
import datetime as dt
import odbblib.schedule as sch

PATH_STR1 = "/data1/prospect/ProcessedData/OrchidAnalysis/TimeSeries_2017"
PATH_STR2 = "/home/itm/test_reader/short_runs"

def read_batch_data(fname):
    """Reads the batch information csv

    Parameters
    ----------
    batch_location : str
        The directory containing the batch data

    Returns
    -------
    batch_data : dict
        Dictionary containing all the values in the raw batch data
    """
    infile = open(fname)
    # skip the header line
    _ = infile.readline()
    data = [x.strip() for x in infile.readline().strip().split(",")]
    batch_data = {}
    # batch name gets checked and possibly modified further down
    _, batch_data["BatchName"] = os.path.split(os.path.split(fname)[0])
    batch_data["DetCount"] = int(data[0])
    batch_data["ArrayX"] = float(data[1])
    batch_data["ArrayY"] = float(data[2])
    batch_data["IntTime"] = float(data[3])
    # start of temporary code for testing on my machine
    # TODO: REMOVE THIS CODE!
    if PATH_STR1 in data[4]:
        data[4] = "/home/jmatta1/test_data" + data[4][len(PATH_STR1):]
    if PATH_STR1 in data[5]:
        data[5] = "/home/jmatta1/test_data" + data[5][len(PATH_STR1):]
    if PATH_STR1 in data[6]:
        data[6] = "/home/jmatta1/test_data" + data[6][len(PATH_STR1):]
    if PATH_STR2 in data[4]:
        data[4] = "/home/jmatta1/test_data/short_runs" + data[4][len(PATH_STR2):]
    if PATH_STR2 in data[5]:
        data[5] = "/home/jmatta1/test_data/short_runs" + data[5][len(PATH_STR2):]
    if PATH_STR2 in data[6]:
        data[6] = "/home/jmatta1/test_data/short_runs" + data[6][len(PATH_STR2):]
    # end of temporary code for testing on my machine
    batch_data["RootFileLocation"] = data[4]
    batch_data["RunDataLocation"] = data[5]
    batch_data["DetDataLocation"] = data[6]
    batch_data["FirstBufferSkipped"] = False if data[7] == "No" else True
    batch_data["TreeGenerated"] = False if data[8] == "No" else True
    batch_data["TreeFileLocation"] = ""
    if batch_data["TreeGenerated"]:
        batch_data["TreeFileLocation"] = data[9]
    batch_data["StartEpochMicroSec"] = int(data[10])
    batch_data["StartDateTime"] = dt.datetime.strptime(data[11],
                                                       "%Y-%b-%d %H:%M:%S.%f")
    batch_data["StopEpochMicroSec"] = int(data[12])
    batch_data["StopDateTime"] = dt.datetime.strptime(data[13],
                                                      "%Y-%b-%d %H:%M:%S.%f")
    batch_data["RunCount"] = int(data[14])
    # ensure that the batch name contains the year in it
    test_year = "{0:d}".format(batch_data["StartDateTime"].year)
    if not test_year in batch_data["BatchName"][-4:]:
        batch_data["BatchName"] += "_{0:s}".format(test_year)
    # add the derived keys to the dictionary
    cycle_nums = sch.get_reactor_cycles(batch_data["StartDateTime"],
                                        batch_data["StopDateTime"])
    batch_data["StartCycleNum"] = cycle_nums[0]
    batch_data["StopCycleNum"] = cycle_nums[1]
    batch_data["StatusNum"] = sch.get_reactor_status(
        batch_data["StartDateTime"], batch_data["StopDateTime"])
    batch_data["StatusName"] = sch.get_reactor_status_name(
        batch_data["StartDateTime"], batch_data["StopDateTime"])
    # add the keys to the dictionary containing the info that is added later
    batch_data["IsCalibrated"] = False
    batch_data["IsDecomposed"] = False
    batch_data["CalRootLoc"] = ""
    batch_data["DecompRootLoc"] = ""
    batch_data["RunDbLoc"] = ""
    return batch_data


def read_run_data(fname, det_data):
    """Reads the run information csv

    Parameters
    ----------
    fname : str
        The path to the csv file with run information

    det_data : list of dicts
        The list of dictionaries containing individual pieces of det info

    Returns
    -------
    run_data : list of dict
        Dictionary containing, for each value in the raw run data, a list of
        that value for every run
    """
    infile = open(fname)
    # skip the header line
    _ = infile.readline()
    run_list = []
    for line in infile:
        data = [x.strip() for x in line.strip().split(',')]
        run_list.append(parse_run_line(data, det_data))
    return run_list


def parse_run_line(data, det_data):
    """Takes a split line of run information and pushes it into a
    dictionary with appropriate keys for the various values

    Parameters
    ----------
    data : list of str
        the line of run data split and stripped of excess whitespace

    det_data : list of dict
        the list of detector data dictionaries

    Returns
    -------
    run_dict_list : list of dicts
        list of information dictionaries for the run, one for general run info
        and one for each detector on the array for detector specific info
    """
    out_list = []
    run_dict = {}
    run_dict["RunNum"] = int(data[0])
    run_dict["StartEpochMicroSec"] = int(data[1])
    run_dict["StartDateTime"] = dt.datetime.strptime(data[2],
                                                     "%Y-%b-%d %H:%M:%S.%f")
    run_dict["StopEpochMicroSec"] = int(data[3])
    run_dict["StopDateTime"] = dt.datetime.strptime(data[4],
                                                    "%Y-%b-%d %H:%M:%S.%f")
    run_dict["CenterEpochMicroSec"] = int(data[5])
    run_dict["CenterDateTime"] = dt.datetime.strptime(data[6],
                                                      "%Y-%b-%d %H:%M:%S.%f")
    run_dict["RunTimeMicroSec"] = int(data[7])
    out_list.append(run_dict)
    start_ind = 8
    for det_dict in det_data:
        out_list.append(parse_det_run_info(data[start_ind:], det_dict,
                                           run_dict["RunNum"]))
        start_ind += 5
    return out_list


def parse_det_run_info(data, det_dict, run_num):
    """Parses the per run individual detector information into a dictionary

    Parameters
    ----------
    data : list of str
        the run data line, stripped and split, starting at the detector of
        interest
    det_dict : dict
        the dictionary with the information for the given detector
    run_num : int
        the run number

    Returns
    -------
    det_run_info : dict
        A dictionary with the per detector run information

    Notes
    -----
    This function relies on the fact that orchidReader outputs the per detector
    information that is in the run info csv in the same order that it is read
    in from the detector data file and that the detector information in the
    """
    det_run_info = {}
    det_run_info["DetNum"] = det_dict["DetNum"]
    det_run_info["RunNum"] = run_num
    det_run_info["AvgVoltage"] = float(data[0])
    det_run_info["AvgCurrentMicroAmps"] = float(data[1])
    det_run_info["AvgHvTempCel"] = float(data[2])
    det_run_info["TotalCounts"] = int(data[3])
    det_run_info["AvgRate"] = float(data[4])
    det_run_info["EnCalOffset"] = 0.0
    det_run_info["EnCalSlope"] = 0.0
    det_run_info["EnCalCurve"] = 0.0
    det_run_info["WidthSqOffset"] = 0.0
    det_run_info["WidthSqSlope"] = 0.0
    det_run_info["WidthSqCurve"] = 0.0
    det_run_info["IsCalibrated"] = False
    det_run_info["IsDecomposed"] = False
    return det_run_info


def read_det_data(fname):
    """Reads the det information csv

    Parameters
    ----------
    fname : str
        The path to the detector meta data file

    Returns
    -------
    det_data : dict
        Dictionary containing, for each value in the det data, a list of
        that value for detector
    """
    infile = open(fname)
    # skip the header line
    _ = infile.readline()
    det_list = []
    for line in infile:
        data = [x.strip() for x in line.strip().split(',')]
        det_list.append(parse_det_line(data))
    return det_list


def parse_det_line(data):
    """Takes a split line of detector information and pushes it into a
    dictionary with appropriate keys for the various values

    Parameters
    ----------
    data_list : list of str
        the line of run data split and stripped of excess whitespace

    det_data : list of dict
        the list of detector data dictionaries

    Returns
    -------
    run_dict : dict
        A dictionary of the information contained in the line
    """
    det_dict = {}
    det_dict["DetNum"] = int(data[0])
    det_dict["DigitizerModule"] = int(data[1])
    det_dict["DigitizerChannel"] = int(data[2])
    det_dict["MpodModule"] = int(data[3])
    det_dict["MpodChannel"] = int(data[4])
    det_dict["DetType"] = data[5]
    det_dict["DetOffsetX"] = float(data[6])
    det_dict["DetPosX"] = float(data[7])
    det_dict["DetOffsetY"] = float(data[8])
    det_dict["DetPosY"] = float(data[9])
    det_dict["DetOffsetZ"] = float(data[10])
    # the z position is the same as the z offset, so we can do this because the
    # orchid reader has a bug that is making it output the x-offset in the
    # z position column, that bug is fixed now, but this fix remains to data
    # need not be reprocessed
    det_dict["DetPosZ"] = float(data[10])
    return det_dict
