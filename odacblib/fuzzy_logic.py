"""This file contains a few functions to handle fuzzy logic operations like
determining which blocks should be summed into overall spectra for figuring out
starting calibrations for the automatic calibration system"""

import datetime as dt
import itertools as itt
import odacblib.schedule as sch

#TODO: handle the possibility of the MIF being present
#TODO: Figure out how to handle reactor startup intermediate points for cal

# Thresholds for reactor on and off for reactor startup, these are set for
# detector 8, other thresholds would be needed for other detectors
NO_MIF_STARTUP_THRESH = [2000.0, 10000.0]
NO_MIF_SHUTDOWN_THRESH = [4000.0, 14000.0]

RX_ON_GAMMAS = [0.5110, 1.173228, 1.322492, 1.460820, 7.63758]
RX_EARLY_OFF_GAMMAS = [1.173228, 1.322492, 1.460820, 2.614511, 2754.007]
RX_OFF_GAMMAS = [1.173228, 1.322492, 1.460820, 2.614511]

def find_sum_ranges(run_info, det_run_data):
    """This function takes the run information and detector run data and uses
    the shedule functions coupled with rates and user input to figure out what
    groupings need to be made to all summing of the data for calibration
    purposes

    Parameters
    ----------
    run_info : list of dicts
        list of dictionaries of non detector specific run data
    det_run_data : list of dicts
        list of dictionaries of detector specific run data

    Returns
    -------
    sum_list : list
        list of tuples where each tuple has the start run, the stop run,
        the gamma-ray list for calibration, and the "kind" of calibration
        0 - reactor on
        1 - reactor off, early (so 24Na peak is visible)
        2 - reactor off, late (no 24Na peak)
    """
    # TODO: Add branching to handle if this is a run with the MIF present
    # first determine if a transition may have happened during this batch
    rx_stat = sch.get_reactor_status(run_info[0]["StartDateTime"],
                                     run_info[-1]["StopDateTime"])
    if rx_stat == 0:
        return check_for_early_shutdown(run_info)
    elif rx_stat == 1:
        return find_startup(run_info, det_run_data)
    elif rx_stat == 2:
        return [(run_info[0]["RunNum"], run_info[-1]["RunNum"], RX_ON_GAMMAS,
                 0)]
    elif rx_stat == 3:
        return find_shutdown(run_info, det_run_data)


def find_shutdown(run_info, det_run_data):
    """This function takes data that may or may not span the reactor shutdown
    It then finds out if it does, and determines calibration sums for that data

    Parameters
    ----------
    run_info : list of dicts
        list of dictionaries of non detector specific run data
    det_run_data : list of dicts
        list of dictionaries of detector specific run data

    Returns
    -------
    sum_list : list
        list of tuples where each tuple has the start run, the stop run,
        the gamma-ray list for calibration, and the "kind" of calibration
        0 - reactor on
        1 - reactor off, early (so 24Na peak is visible)
        2 - reactor off, late (no 24Na peak)
    """
    # first determine which index contains detector 8
    ind = [x[0]["DetNum"] for x in det_run_data].index(8)
    # use the thresholds for detector 8 to figure out what runs are running
    # and what runs are not running and what runs are in between
    status = []
    for run in det_run_data[ind]:
        if run["AvgRate"] < NO_MIF_SHUTDOWN_THRESH[0]:
            status.append(0)
        elif run["AvgRate"] > NO_MIF_SHUTDOWN_THRESH[1]:
            status.append(2)
        else:
            status.append(1)
    off_range = [i for i in range(len(status)) if status[i] == 0]
    on_range = [run_info[i]["RunNum"] for i in range(len(status))
                if status[i] == 2]
    sum_list = []
    if len(on_range) != 0:
        sum_list.append((on_range[0], on_range[-1], RX_ON_GAMMAS, 0))
    if len(off_range) != 0:
        sum_list.extend(check_for_early_shutdown(run_info[off_range[0]:]))
    return sum_list


def check_for_early_shutdown(run_info):
    """This function takes data that may or may not span the reactor shutdown
    It then finds out if it does, and determines calibration sums for that data

    Parameters
    ----------
    run_info : list of dict
        list of dictionaries of non detector specific run data

    Returns
    -------
    sum_list : list
        list of tuples where each tuple has the start run, the stop run,
        the gamma-ray list for calibration, and the "kind" of calibration
        0 - reactor on
        1 - reactor off, early (24Na peak is visible)
        2 - reactor off, late (no 24Na peak)
    """
    # first get the relevant shutdown day from the schedule
    shutdown_date = sch.get_previous_shutdown(run_info[0]["StartDateTime"])
    ref_date = (shutdown_date + dt.timedelta(days=3))
    near_list = [val for val in run_info if val["StartDateTime"] < ref_date]
    far_list = [val for val in run_info if val["StartDateTime"] >= ref_date]
    out_list = []
    if len(near_list) != 0:
        out_list.append((near_list[0]["RunNum"], near_list[-1]["RunNum"],
                         RX_EARLY_OFF_GAMMAS, 1))
    if len(far_list) != 0:
        out_list.append((far_list[0]["RunNum"], far_list[-1]["RunNum"],
                         RX_OFF_GAMMAS, 2))
    return out_list


def find_startup(run_info, det_run_data):
    """This function takes data that may or may not span the reactor shutdown
    It then finds out if it does, and determines calibration sums for that data

    Parameters
    ----------
    run_info : list of dict
        list of dictionaries of non detector specific run data
    det_run_data : list of dicts
        list of dictionaries of detector specific run data

    Returns
    -------
    sum_list : list
        list of tuples where each tuple has the start run, the stop run,
        the gamma-ray list for calibration, and the "kind" of calibration
        0 - reactor on
        1 - reactor off, early (so 24Na peak is visible)
        2 - reactor off, late (no 24Na peak)
    """
    # first determine which index contains detector 8
    ind = [x[0]["DetNum"] for x in det_run_data].index(8)
    # use the thresholds for detector 8 to figure out what runs are running
    # and what runs are not running and what runs are in between
    status = []
    for run in det_run_data[ind]:
        if run["AvgRate"] < NO_MIF_STARTUP_THRESH[0]:
            status.append(0)
        elif run["AvgRate"] > NO_MIF_STARTUP_THRESH[1]:
            status.append(2)
        else:
            status.append(1)
    not_on_range = [run_info[i]["RunNum"] for i in range(len(status))
                 if status[i] in [0, 1]]
    on_range = [run_info[i]["RunNum"] for i in range(len(status))
                if status[i] == 2]
    # now group the off range since it could be interrupted by a brief burst of
    # intermediate strength reactor on
    temp = itt.groupby(enumerate(not_on_range), lambda (i, x): status[i])
    off_range = []
    for k, g in temp:
        if k == 0:
            arr = [r for r in g]
            off_range.append(arr)
    sum_list = []
    if len(off_range) != 0:
        for subr in off_range:
            sum_list.append((subr[0][1], subr[-1][1], RX_OFF_GAMMAS, 2))
    if len(on_range) != 0:
        sum_list.append((on_range[0], on_range[-1], RX_ON_GAMMAS, 0))
    return sum_list
