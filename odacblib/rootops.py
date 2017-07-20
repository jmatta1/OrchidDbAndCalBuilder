"""This file contains the functions that are used to directly access and
manipulate root files and data"""

import ctypes as ct
import ROOT as rt
import odacblib.input_sanitizer as ins

def find_sodium_peak_runs(lo_bnd, hi_bnd, root_input):
    """This function prepares a calibration root file for a single calibration
    block, when that block happens to fall into the "early reactor off" class
    It will query the user to deterimine where the 24Na peak dissappears
    and produce two run data tuples, one with and one without the 24Na peak

    Parameters
    ----------
    lo_bnd : int
        The lowest run in the stretch to be checked
    hi_bnd : int
        The highest run in the stretch to be checked
    root_input : str
        The path of the root input file

    Returns
    -------
    runs_list : list of tuples
        Each tuple contains a start run and stop run and either a 1 or a 0
        0 indicates there is no 24Na peak in these runs, 1 indicates there is
    """
    query = "Does this spectrum contain a reasonable 24Na peak"
    # First create a canvas so that we can control drawing
    canv = rt.TCanvas("c1", "Find 24Na peaks")
    print "Finding Runs containing a reasonable 24Na peak"
    infile = rt.TFile(root_input)
    # first check if there is a 24Na peak at the start of everything
    fmt = "Det_8_Run_{0:d}_px"
    out_list = []
    print "Checking spectrum:", lo_bnd
    rt.gStyle.SetOptStat(0)
    hist = infile.Get(fmt.format(lo_bnd))
    hist.Draw()
    canv.Update()
    last_ans = ins.get_yes_no(query, default_value=True)
    start_run = lo_bnd
    stop_run = lo_bnd
    x_range = (canv.GetFrame().GetX1(), canv.GetFrame().GetX2())
    for i in range(lo_bnd+1, hi_bnd+1):
        print "Checking spectrum:", i
        hist = infile.Get(fmt.format(i))
        hist.Draw()
        canv.Update()
        hist.GetXaxis().SetRangeUser(x_range[0], x_range[1])
        hist.Draw()
        canv.Update()
        ans = ins.get_yes_no(query, default_value=True)
        x_range = (canv.GetFrame().GetX1(), canv.GetFrame().GetX2())
        if last_ans == ans:
            stop_run = i
        else:
            out_list.append((start_run, stop_run, 1 if last_ans else 0))
            start_run = i
            stop_run = i
            last_ans = ans
    out_list.append((start_run, stop_run, 1 if last_ans else 0))
    return out_list


def prep_calibration_file(runs, root_input, root_output, det_data, num_runs):
    """This function  generates / copies sums for the calibration file for the
    full calibration program to use

    Parameters
    ----------
    runs : list of tuples
        list of tuples where each tuple has the start run, the stop run,
        the gamma-ray list for calibration, and the "kind" of calibration
        0 - reactor on
        1 - reactor off, early (so 24Na peak is visible)
        2 - reactor off, late (no 24Na peak)
    root_input : str
        The path of the root input file
    root_output : str
        The path of the root output file
    det_data : list of dict
        list of dictionary of the detector data
    num_runs : int
        The number of runs in this batch
    """
    # first check if we can merely use pregenerated sums or if we need to
    # generate new sums
    run_count = 1 + runs[0][1] - runs[0][0]
    if len(runs) == 1 and run_count == num_runs:
        do_normal_prep(runs, root_input, root_output, det_data)
    else:
        do_split_prep(runs, root_input, root_output, det_data)


def do_split_prep(runs, root_input, root_output, det_data):
    """This function prepares a calibration root file for a single calibration
    block, i.e. all the data is coming from reactor on, or reactor off, no
    exceptions

    Parameters
    ----------
    runs : list of tuples
        list of tuples where each tuple has the start run, the stop run,
        the gamma-ray list for calibration, and the "kind" of calibration
        0 - reactor on
        1 - reactor off, early (so 24Na peak is visible)
        2 - reactor off, late (no 24Na peak)
    root_input : str
        The path of the root input file
    root_output : str
        The path of the root output file
    det_data : list of dict
        list of dictionary of the detector data
    """
    print "Preparing Root Calibration File"
    for i, run in enumerate(runs):
        do_single_prep(run, root_input, root_output, det_data, i)
    outfile = rt.TFile(root_output, "UPDATE")
    outfile.cd()
    num_cals = rt.TParameter('int')("NumCals", 1)
    num_cals.Write()
    outfile.Flush()
    print "Done preparing calibration sums"


def do_single_prep(run, root_input, root_output, det_data, ind):
    """This function prepares a calibration root file for a single calibration
    block, i.e. all the data is coming from reactor on, or reactor off, no
    exceptions

    Parameters
    ----------
    run : tuple
        tuple where each tuple has the start run, the stop run,
        the gamma-ray list for calibration, and the "kind" of calibration
        0 - reactor on
        1 - reactor off, early (so 24Na peak is visible)
        2 - reactor off, late (no 24Na peak)
    root_input : str
        The path of the root input file
    root_output : str
        The path of the root output file
    det_data : list of dict
        list of dictionary of the detector data
    ind : int
        which calibration we are preparing
    """
    print "Preparing Calibration #{0:d}".format(ind)
    lib = ct.cdll.LoadLibrary("./odacblib/libCalibrate.so")
    lib.performCalSum.argtypes = [ct.c_int, ct.c_int, ct.c_int, ct.c_int,
                                    ct.c_char_p, ct.c_char_p, ct.c_int]
    # now copy the sum spectra over to the root file
    count = 0
    for dat in det_data:
        nf = 1
        if count==0 and ind == 0:
            nf = 0
        print "    Preparing Sums For Det #{0:d}".format(dat["DetNum"])
        lib.performCalSum(run[0], run[1], dat["DetNum"], ind,
                          ct.c_char_p(root_input), ct.c_char_p(root_output),
                          nf)
        count += 1
    outfile = rt.TFile(root_output, "UPDATE")
    # write a few extra tidbits to the file
    cal_start = rt.TParameter('int')("Cal_{0:d}_Start".format(ind), run[0])
    cal_start.Write()
    cal_stop = rt.TParameter('int')("Cal_{0:d}_Stop".format(ind), run[1])
    cal_stop.Write()
    num_gammas = rt.TParameter('int')("Cal_{0:d}_NumGammas".format(ind),
                                      len(run[2]))
    num_gammas.Write()
    for i, gamma in enumerate(run[2]):
        temp = rt.TParameter('double')("Cal_{0:d}_Gamma_{1:d}".format(ind, i),
                                       gamma)
        temp.Write()
    outfile.Flush()


def do_normal_prep(runs, root_input, root_output, det_data):
    """This function prepares a calibration root file for a single calibration
    block, i.e. all the data is coming from reactor on, or reactor off, no
    exceptions

    Parameters
    ----------
    runs : list of tuples
        list of tuples where each tuple has the start run, the stop run,
        the gamma-ray list for calibration, and the "kind" of calibration
        0 - reactor on
        1 - reactor off, early (so 24Na peak is visible)
        2 - reactor off, late (no 24Na peak)
    root_input : str
        The path of the root input file
    root_output : str
        The path of the root output file
    det_data : list of dict
        list of dictionary of the detector data
    """
    # otherwise we can procded as normal
    # open the root files
    print "Preparing Root Calibration File"
    infile = rt.TFile(root_input)
    outfile = rt.TFile(root_output, "RECREATE")
    # write a few extra tidbits to the file
    num_cals = rt.TParameter('int')("NumCals", 1)
    num_cals.Write()
    cal_start = rt.TParameter('int')("Cal_0_Start", runs[0][0])
    cal_start.Write()
    cal_stop = rt.TParameter('int')("Cal_0_Stop", runs[0][1])
    cal_stop.Write()
    num_gammas = rt.TParameter('int')("Cal_0_NumGammas", len(runs[0][2]))
    num_gammas.Write()
    for i, gamma in enumerate(runs[0][2]):
        temp = rt.TParameter('double')("Cal_0_Gamma_{0:d}".format(i), gamma)
        temp.Write()
    outfile.Flush()
    # Now prepare to write the spectra to the file
    in_fmts = ["Det_{0:d}_Sum_px", "Det_{0:d}_Sum_px_thresh",
               "Det_{0:d}_Sum_py", "Det_{0:d}_Sum_px_thresh",
               "Det_{0:d}_Sum_2D"]
    out_fmts = ["Det_{0:d}_Sum_px_Cal_0", "Det_{0:d}_Sum_px_thresh_Cal_0",
                "Det_{0:d}_Sum_py_Cal_0", "Det_{0:d}_Sum_px_thresh_Cal_0",
                "Det_{0:d}_Sum_2D_Cal_0"]
    # now copy the sum spectra over to the root file
    for dat in det_data:
        # first copy the histograms over
        in_names = [x.format(dat["DetNum"]) for x in in_fmts]
        out_names = [x.format(dat["DetNum"]) for x in out_fmts]
        for iname, oname in zip(in_names, out_names):
            hist = infile.Get(iname)
            hist.SetName(oname)
            outfile.cd()
            hist.Write()
    outfile.Flush()


def get_sum_cal_fits(runs, root_output, det_data):
    """This function prepares a calibration root file for a single calibration
    block, i.e. all the data is coming from reactor on, or reactor off, no
    exceptions

    Parameters
    ----------
    runs : list of tuples
        list of tuples where each tuple has the start run, the stop run,
        the gamma-ray list for calibration, and the "kind" of calibration
        0 - reactor on
        1 - reactor off, early (so 24Na peak is visible)
        2 - reactor off, late (no 24Na peak)
    root_input : str
        The path of the root input file
    """
    pass
