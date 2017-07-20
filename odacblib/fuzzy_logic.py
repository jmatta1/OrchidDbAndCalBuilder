"""This file contains a few functions to determine which blocks should be
summed into overall spectra for figuring out starting calibrations for the
automatic calibration system"""

import odacblib.schedule as sch

#TODO: handle the possibility of the MIF being present

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
    run_data : dict
        dictionary of non detector specific run data
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
        return check_for_early_shutdown(run_info, det_run_data)
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
    run_data : dict
        dictionary of non detector specific run data
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
    # extract trigger rates from detector 8
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
    on_range = [run_info[i]["RunNum"] for i in range(len(status)) if status[i] == 2]
    sum_list = [(on_range[0], on_range[-1], RX_ON_GAMMAS, 0)]
    print sum_list
    off_list = check_for_early_shutdown(run_info[off_range[0]:],
                                        det_run_data[off_range[0]:])
    print off_list
    sum_list.extend(off_list)
    return sum_list


def check_for_early_shutdown(run_info, det_run_data):
    """This function takes data that may or may not span the reactor shutdown
    It then finds out if it does, and determines calibration sums for that data

    Parameters
    ----------
    run_data : dict
        dictionary of non detector specific run data
    det_run_data : list of dicts
        list of dictionaries of detector specific run data

    Returns
    -------
    sum_list : list
        list of tuples where each tuple has the start run, the stop run,
        the gamma-ray list for calibration, and the "kind" of calibration
        0 - reactor on
        1 - reactor off, early (24Na peak is visible)
        2 - reactor off, late (no 24Na peak)
    """
    
    return []


def find_startup(run_info, det_run_data):
    """This function takes data that may or may not span the reactor shutdown
    It then finds out if it does, and determines calibration sums for that data

    Parameters
    ----------
    run_data : dict
        dictionary of non detector specific run data
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
    return []
