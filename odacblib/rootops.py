"""This file contains the functions that are used to directly access and
manipulate root files and data"""

import ROOT as rt

def prep_calibration_file(sum_lists, root_input, root_output, run_info):
    """This function  generates / copies sums for the calibration file for the
    full calibration program to use

    Parameters
    ----------
    sum_lists : list of tuples
        list of tuples where each tuple has the start run, the stop run,
        the gamma-ray list for calibration, and the "kind" of calibration
        0 - reactor on
        1 - reactor off, early (so 24Na peak is visible)
        2 - reactor off, late (no 24Na peak)
    root_input : str
        The path of the root input file
    root_output : str
        The path of the root output file
    """
    # first check if we can merely use pregenerated sums or if we need to
    # generate new sums
