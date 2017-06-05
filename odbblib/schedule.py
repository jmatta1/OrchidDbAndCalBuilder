"""Contains the HFIR past operating schedule and functions for determining
where within that schedule we are"""
import datetime as dt

HFIR_STARTUP_DAYS = [dt.datetime(2016, 9, 5), dt.datetime(2016, 11, 15),
                     dt.datetime(2017, 1, 3), dt.datetime(2017, 2, 14),
                     dt.datetime(2017, 5, 3), dt.datetime(2017, 6, 13)]


HFIR_SHUTDOWN_DAYS = [dt.datetime(2016, 9, 27), dt.datetime(2016, 12, 8),
                      dt.datetime(2017, 1, 27), dt.datetime(2017, 3, 10),
                      dt.datetime(2017, 5, 27)]


HFIR_CYCLE_NUM = [468, 469, 470, 471, 472]

def get_cycle_num(batch_start, batch_stop):
    """This function returns a tuple containing two numbers, if both the batch
    start and batch stop are within the same cycle then both numbers are the
    same and are equal to the HFIR cycle number containing those dates

    Parameters
    ----------
    batch_start : datetime.datetime
        datetime of the start of the batch
    batch_stop : datetime.datetime
        datetime of the end of the batch

    Returns
    -------
    first_cycle : int
        The cycle number in the first part of the batch
    second_cycle : int
        The cycle number in the second part of the batch
    """
    pass


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
    pass


