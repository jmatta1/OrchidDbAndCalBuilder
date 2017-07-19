"""This file contains a few functions to determine which blocks should be
summed into overall spectra for figuring out starting calibrations for the
automatic calibration system"""

def find_sum_ranges(run_info, det_run_data):
    """This function takes the run information and detector run data and uses
    the shedule functions coupled with rates and user input to figure out what
    groupings need to be made to all summing of the data for calibration
    purposes

    Parameters
    ----------
    run_data : dict
        dictionary of non detector specific run data
    det_run_data : list of dicts
        list of dictionaries of detector specific run data
    """
    # extract trigger rates from detector 8
    # first determine which index contains detector 8
    ind = [x[0]["DetNum"] for x in det_run_data].index(8)
    # make a list of the detector 9 run times and rates
    run_info = [(run_info[i]["StartEpochMicroSec"],
                 run_info[i]["StopEpochMicroSec"],
                 run_info[i]["CenterEpochMicroSec"],
                 det_run_data[ind][i]["AvgRate"])
                for i in range(len(run_info))]
    
