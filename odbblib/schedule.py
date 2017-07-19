"""Contains the HFIR past operating schedule and functions for determining
where within that schedule we are"""
import datetime as dt

HFIR_STARTUP_DAYS = [dt.datetime(2016, 9, 6, 9, 57),
                     dt.datetime(2016, 11, 15, 9, 42),
                     dt.datetime(2017, 1, 3, 14, 21),
                     dt.datetime(2017, 2, 14, 8, 30),
                     dt.datetime(2017, 5, 3, 12, 50),
                     dt.datetime(2017, 6, 13, 7, 58),
                     dt.datetime(2017, 7, 25, 0, 0),  # in the future
                     dt.datetime(2017, 9, 5, 0, 0)]  # in the future

HFIR_SHUTDOWN_DAYS = [dt.datetime(2016, 9, 30, 10, 4),
                      dt.datetime(2016, 12, 8, 20, 15),
                      dt.datetime(2017, 1, 29, 0, 0),
                      dt.datetime(2017, 3, 11, 23, 59),
                      dt.datetime(2017, 5, 27, 22, 35),
                      dt.datetime(2017, 7, 8, 14, 29),
                      dt.datetime(2017, 8, 18, 0, 0),  # in the future
                      dt.datetime(2017, 9, 29, 0, 0)]  # in the future

HFIR_CYCLE_NUM = [468, 469, 470, 471, 472, 473, 474]

CYCLE_STATUS_NAMES = ["Reactor Off", "Reactor Startup", "Reactor On",
                      "Reactor Shutdown"]

# variables automatically generated from the variables above

#the mixed reactor starts and stops
HFIR_MIXED_DAYS = list(sum(zip(HFIR_STARTUP_DAYS, HFIR_SHUTDOWN_DAYS), ()))

#this should be equal to the length of whatever the longest array above is
UPPER_ERROR = len(HFIR_MIXED_DAYS)


def find_range(batch_start, batch_stop):
    """This finds the last date in the interleaved date data where batch_start
    and batch stop are *after* that date

    Parameters
    ----------
    batch_start : datetime.datetime
        datetime of the start of the batch
    batch_stop : datetime.datetime
        datetime of the end of the batch

    Returns
    -------
    start_ind : int
        The index of the last date that batch_start is after
    stop_ind : int
        The index of the last date that batch_stop is after
    """
    start_test_list = [(batch_start < x) for x in HFIR_MIXED_DAYS]
    stop_test_list = [(batch_stop < x) for x in HFIR_MIXED_DAYS]
    start_ind = -2
    for i, val in enumerate(start_test_list):
        if val:
            start_ind = i - 1
            break
    if start_ind == -2:
        start_ind = UPPER_ERROR
    stop_ind = -2
    for i, val in enumerate(stop_test_list):
        if val:
            stop_ind = i - 1
            break
    if stop_ind == -2:
        stop_ind = UPPER_ERROR
    return (start_ind, stop_ind)


def get_reactor_status(batch_start, batch_stop):
    """This function takes the batch start and stop and determines the reactor
    status across that period.

    Parameters
    ----------
    batch_start : datetime.datetime
        datetime of the start of the batch
    batch_stop : datetime.datetime
        datetime of the end of the batch

    Returns
    -------
    RxStatus : int
        an integer representing the reactor status
        0 - Reactor Off
        1 - Reactor Transition Off to On
        2 - Reactor On
        3 - Reactor Transition On to Off
    """
    # handle stupid error
    if batch_start >= batch_stop:
        print "Error: batch start is either the same as or later than batch_stop"
        return None
    (start_ind, stop_ind) = find_range(batch_start, batch_stop)
    # handle error cases
    if (stop_ind - start_ind) > 1:
        print "Error: batch start and stop span a reactor turn on and off"
        return None
    if start_ind >= UPPER_ERROR:
        print "Error: both start and stop time are past the known cycle times"
        print "    Expand the time listings in odbblib/schedule.py"
        print "      HFIR_STARTUP_DAYS and HFIR_SHUTDOWN_DAYS"
        return None
    if stop_ind == -1:
        print "Error: both start and stop time are before the known cycle times"
        print "    Expand the time listings in odbblib/schedule.py"
        print "      HFIR_STARTUP_DAYS and HFIR_SHUTDOWN_DAYS"
        return None
    bstart = batch_start.date()
    bstop = batch_stop.date()
    if (bstart == HFIR_MIXED_DAYS[start_ind].date() and
            bstop == HFIR_MIXED_DAYS[stop_ind].date()):
        print "Error: batch start and stop span a reactor turn on and off"
        return None
    # handle the cases that are not errors
    ret_val = 3
    if bstart == HFIR_MIXED_DAYS[start_ind].date() and start_ind%2 == 1:
        ret_val = 3
    elif stop_ind == start_ind:
        if bstart == HFIR_MIXED_DAYS[start_ind].date() and (start_ind%2) == 0:
            ret_val = 1
        elif bstop == HFIR_MIXED_DAYS[stop_ind+1].date() and (start_ind%2) == 1:
            ret_val = 1
        elif (stop_ind%2) == 1:
            ret_val = 0
        else:
            ret_val = 2
    elif (start_ind%2) == 1:
        ret_val = 1
    return ret_val


def get_reactor_status_name(batch_start, batch_stop):
    """This function takes the batch start and stop and determines the reactor
    status across that period, returning a name for that status

    Parameters
    ----------
    batch_start : datetime.datetime
        datetime of the start of the batch
    batch_stop : datetime.datetime
        datetime of the end of the batch

    Returns
    -------
    RxStatus : str
        a string representing the reactor status
    """
    return CYCLE_STATUS_NAMES[get_reactor_status(batch_start, batch_stop)]


def get_reactor_cycles(batch_start, batch_stop):
    """This function takes the batch start and stop and determines the reactor
    cycle number at the start and end of the batch

    Parameters
    ----------
    batch_start : datetime.datetime
        datetime of the start of the batch
    batch_stop : datetime.datetime
        datetime of the end of the batch

    Returns
    -------
    start_cycle : int
        the cycle number at the start of the batch
    stop_cycle : int
        the cycle number at the end of the batch
    """
    (start_ind, stop_ind) = find_range(batch_start, batch_stop)
    return (HFIR_CYCLE_NUM[start_ind/2], HFIR_CYCLE_NUM[stop_ind/2])
