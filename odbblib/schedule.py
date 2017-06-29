"""Contains the HFIR past operating schedule and functions for determining
where within that schedule we are"""
import datetime as dt

HFIR_STARTUP_DAYS = [dt.date(2016, 9, 5), dt.date(2016, 11, 15),
                     dt.date(2017, 1, 3), dt.date(2017, 2, 14),
                     dt.date(2017, 5, 3), dt.date(2017, 6, 13),
                     dt.date(2017, 7, 25), dt.date(2017, 9, 5)]

HFIR_SHUTDOWN_DAYS = [dt.date(2016, 9, 27), dt.date(2016, 12, 8),
                      dt.date(2017, 1, 27), dt.date(2017, 3, 10),
                      dt.date(2017, 5, 27), dt.date(2017, 7, 7),
                      dt.date(2017, 8, 18), dt.date(2017,9, 29)]

HFIR_CYCLE_NUM = [468, 469, 470, 471, 472, 473, 474]

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
    start_test_list = [(batch_start.date() < x) for x in HFIR_MIXED_DAYS]
    stop_test_list = [(batch_stop.date() < x) for x in HFIR_MIXED_DAYS]
    print start_test_list
    print stop_test_list
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
    if(bstart == HFIR_MIXED_DAYS[start_ind] and
       bstop == HFIR_MIXED_DAYS[stop_ind]):
        print "Error: batch start and stop span a reactor turn on and off"
        return None
    # handle the cases that are not errors
    if (bstart == HFIR_MIXED_DAYS[start_ind] and start_ind%2 == 1):
        return 3
    if stop_ind == start_ind:
        if (bstart == HFIR_MIXED_DAYS[start_ind]):
            if (start_ind%2) == 1:
                return 3
            else:
                return 1
        elif (bstop == HFIR_MIXED_DAYS[stop_ind+1]):
            if (start_ind%2) == 1:
                return 1
            else:
                return 3
        elif (stop_ind%2) == 1:
            return 0
        else:
            return 2
    else:
        if (start_ind%2) == 1:
            return 1
        else:
            return 3


if __name__ == "__main__":
    temp1 = dt.datetime(2016, 9, 6, 9, 33)
    temp2 = dt.datetime(2016, 9, 10, 7, 13)
    print temp1
    print temp2
    print HFIR_MIXED_DAYS
    print find_range(temp1, temp2)
    print get_reactor_status(temp1, temp2)
    temp3 = dt.datetime(2017, 6, 1, 10, 15)
    temp4 = dt.datetime(2017, 6, 15, 8, 19)
    print temp3
    print temp4
    print HFIR_MIXED_DAYS
    print find_range(temp3, temp4)
    print get_reactor_status(temp3, temp4)

