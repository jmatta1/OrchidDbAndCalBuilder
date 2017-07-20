"""Functions to create, update, and add to the various databases"""
import sqlite3 as sql
import sys
import datetime as dt
import odacblib.input_sanitizer as ins

BATCH_TABLE_CMD = """CREATE TABLE batch_table (
    batch_name text PRIMARY KEY,
    start_us_epoch int NOT NULL,
    stop_us_epoch int NOT NULL,
    start_time text NOT NULL,
    stop_time text NOT NULL,
    array_x real NOT NULL,
    array_y real NOT NULL,
    raw_root_location text NOT NULL,
    run_data_location text NOT NULL,
    det_data_location text NOT NULL,
    tree_gen int NOT NULL,
    tree_root_location text NOT NULL,
    hist_integration_time real NOT NULL,
    run_count int NOT NULL,
    det_count int NOT NULL,
    first_buffer_skip int NOT NULL,
    start_cycle_number int NOT NULL,
    stop_cycle_number int NOT NULL,
    reactor_status_number int NOT NULL,
    reactor_status_desc int NOT NULL,
    has_been_calibrated int DEFAULT 0,
    has_been_decomposed int DEFAULT 0,
    cal_root_location text DEFAULT '',
    decomp_root_location text DEFAULT '',
    run_db_location text DEFAULT ''
);
"""

BATCH_UPDATE = """UPDATE batch_table
SET start_us_epoch = ?,
    stop_us_epoch = ?,
    start_time = ?,
    stop_time = ?,
    array_x = ?,
    array_y = ?,
    raw_root_location = ?,
    run_data_location = ?,
    det_data_location = ?,
    tree_gen = ?,
    tree_root_location = ?,
    hist_integration_time = ?,
    run_count = ?,
    det_count = ?,
    first_buffer_skip = ?,
    start_cycle_number = ?,
    stop_cycle_number = ?,
    reactor_status_number = ?,
    reactor_status_desc = ?,
    has_been_calibrated = ?,
    has_been_decomposed = ?,
    cal_root_location = ?,
    decomp_root_location = ?,
    run_db_location text = ?
WHERE
    batch_name = ?
;
"""

BATCH_INSERT = "INSERT INTO batch_table VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, "\
    "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"

BATCH_SELECT = "SELECT * FROM batch_table WHERE batch_name='{0:s}';"

BATCH_DICT_NAMES = ["BatchName", "StartEpochMicroSec", "StopEpochMicroSec",
                    "StartDateTime", "StopDateTime", "ArrayX", "ArrayY",
                    "RootFileLocation", "RunDataLocation", "DetDataLocation",
                    "TreeGenerated", "TreeFileLocation", "IntTime", "RunCount",
                    "DetCount", "FirstBufferSkipped", "StartCycleNum",
                    "StopCycleNum", "StatusNum", "StatusName", "IsCalibrated",
                    "IsDecomposed", "CalRootLoc", "DecompRootLoc", "RunDbLoc"]


MAKE_DET_DATA_TABLE = """CREATE TABLE det_data_table (
    detector_number int PRIMARY KEY,
    digitizer_module int NOT NULL,
    digitizer_channel int NOT NULL,
    mpod_module int NOT NULL,
    mpod_channel int NOT NULL,
    detector_type text NOT NULL,
    detector_offset_x real NOT NULL,
    detector_position_x real NOT NULL,
    detector_offset_y real NOT NULL,
    detector_position_y real NOT NULL,
    detector_offset_z real NOT NULL,
    detector_position_z real NOT NULL
) WITHOUT ROWID;
"""

DET_INSERT = "INSERT INTO det_data_table VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?,"\
    " ?, ?, ?)"

DET_DATA_NAMES = ["DetNum", "DigitizerModule", "DigitizerChannel", "MpodModule",
                  "MpodChannel", "DetType", "DetOffsetX", "DetPosX",
                  "DetOffsetY", "DetPosY", "DetOffsetZ", "DetPosZ"]


MAKE_RUN_TABLE = """CREATE TABLE run_data_table (
    run_number int PRIMARY KEY,
    start_us_epoch int NOT NULL,
    stop_us_epoch int NOT NULL,
    center_us_epoch int NOT NULL,
    run_time_us int NOT NULL,
    start_time text NOT NULL,
    stop_time text NOT NULL,
    center_time text NOT NULL
) WITHOUT ROWID;
"""

RUN_INSERT = "INSERT INTO run_data_table VALUES (?, ?, ?, ?, ?, ?, ?, ?)"

RUN_DATA_NAMES = ["RunNum", "StartEpochMicroSec", "StopEpochMicroSec",
                  "CenterEpochMicroSec", "RunTimeMicroSec", "StartDateTime",
                  "StopDateTime", "CenterDateTime"]

MAKE_DET_RUN_TABLE = """CREATE TABLE {0:s} (
    run_number int PRIMARY KEY,
    avg_voltage real NOT NULL,
    avg_current_ua real NOT NULL,
    avg_hv_temp real NOT NULL,
    integral_counts int NOT NULL,
    avg_rate real NOT NULL,
    en_cal_offset real NOT NULL,
    en_cal_slope real NOT NULL,
    en_cal_curve real NOT NULL,
    widthsq_offset real NOT NULL,
    widthsq_slope real NOT NULL,
    widthsq_curve real NOT NULL,
    is_calibrated int NOT NULL,
    is_decomposed int NOT NULL
) WITHOUT ROWID;
"""

DET_RUN_INSERT = "INSERT INTO {0:s} VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "\
                 "?, ?, ?, ?)"

DET_RUN_NAMES = ["RunNum", "AvgVoltage", "AvgCurrentMicroAmps", "AvgHvTempCel",
                 "TotalCounts", "AvgRate", "EnCalOffset", "EnCalSlope",
                 "EnCalCurve", "WidthSqOffset", "WidthSqSlope", "WidthSqCurve",
                 "IsCalibrated", "IsDecomposed"]

def make_batch_database(run_db_path, det_data, run_data, det_run_data):
    """Creates the run information database from the base data

    Parameters
    ----------
    run_db_path : str
        Path to the run database file to be created
    det_data : list of dict
        list of dictionary of the detector data
    run_data : dict
        dictionary of non detector specific run data
    det_run_data : list of dicts
        list of dictionaries of detector specific run data
    """
    # if the database did not already exist it will be created in the connect
    dbcon = sql.connect(run_db_path)
    cursor = dbcon.cursor()
    # make the detector info table
    make_det_table(dbcon, cursor, det_data)
    # make the run table
    make_run_table(dbcon, cursor, run_data)
    # make the detector run tables
    make_det_run_tables(dbcon, cursor, det_run_data)
    # now optimize the database
    cursor.execute("VACUUM")
    dbcon.commit()
    dbcon.close()
    print "Added run information to local batch database"


def make_det_run_tables(dbcon, cursor, det_run_data):
    """Creates a table for each run that contains the relavent information for
    that detector in that run

    Parameters
    ----------
    dbcon : sqlite database connection
        The connection to the sqlite run database
    cursor : splite cursor
        The cursor into the sqlite database
    det_run_data : list of lists of dicts
        The list of lists of per run detector information
    """
    for data in det_run_data:
        # create the name of the database
        table_name = "det_{0:02d}_run_table".format(data[0]["DetNum"])
        make_tbl_cmd = MAKE_DET_RUN_TABLE.format(table_name)
        try:
            cursor.execute(make_tbl_cmd)
        except sql.OperationalError:
            # if there was an error creating the table then it already exists
            print "\n{0:s} already exists for this batch".format(table_name)
            print "    1 - Abort execution"
            print "    2 - Recreate {0:s}".format(table_name)
            print "    3 - Skip Writing {0:s}".format(table_name)
            ans = ins.get_int("Enter Option Number:", inclusive_lower_bound=1,
                              inclusive_upper_bound=3, default_value=3)
            if ans == 1:
                print "Aborting Execution"
                sys.exit()
            elif ans == 2:
                print "Recreating {0:s}".format(table_name)
                cursor.execute("DROP TABLE {0:s}".format(table_name))
                cursor.execute(make_tbl_cmd)
            elif ans == 3:
                print "Skipping writing of {0:s}".format(table_name)
                continue
        # now insert the data into the table
        insert_cmd = DET_RUN_INSERT.format(table_name)
        for run in data:
            insert_list = generate_insert_list(run, DET_RUN_NAMES)
            cursor.execute(insert_cmd, insert_list)
        dbcon.commit()


def make_run_table(dbcon, cursor, run_data):
    """Takes the list of run data dictionaries and dumps them to the run data
    table

    Parameters
    ----------
    dbcon : sqlite database connection
        The connection to the sqlite run database
    cursor : splite cursor
        The cursor into the sqlite database
    run_data : list of dicts
        The list of run data dictionaries
    """
    # check if the table exists (in case the db is newly created)
    try:
        cursor.execute(MAKE_RUN_TABLE)
    except sql.OperationalError:
        # if there was an error creating the table then it already exists
        print "\nRun info table already exists for this batch"
        print "    1 - Abort execution"
        print "    2 - Recreate run_data_table"
        print "    3 - Skip Writing run_data_table"
        ans = ins.get_int("Enter Option Number:", inclusive_lower_bound=1,
                          inclusive_upper_bound=3, default_value=3)
        if ans == 1:
            print "Aborting Execution"
            sys.exit()
        elif ans == 2:
            print "Recreating run_data_table"
            cursor.execute("DROP TABLE run_data_table")
            cursor.execute(MAKE_RUN_TABLE)
        elif ans == 3:
            print "Skipping writing of run_data_table"
            return
    # now insert the data into the table
    for data in run_data:
        insert_list = generate_insert_list(data, RUN_DATA_NAMES)
        cursor.execute(RUN_INSERT, insert_list)
    dbcon.commit()


def make_det_table(dbcon, cursor, det_data):
    """Takes the list of detector data dictionaries and puts them in the
    appropriate table

    Parameters
    ----------
    dbcon : sqlite database connection
        The connection to the sqlite run database
    cursor : splite cursor
        The cursor into the sqlite database
    det_data : list of dicts
        The list of detector data dictionaries
    """
    # check if the table exists (in case the db is newly created)
    try:
        cursor.execute(MAKE_DET_DATA_TABLE)
    except sql.OperationalError:
        # if there was an error creating the table then it already exists
        print "\nDet info table already exists for this batch"
        print "    1 - Abort execution"
        print "    2 - Recreate det_data_table"
        print "    3 - Skip Writing det_data_table"
        ans = ins.get_int("Enter Option Number:", inclusive_lower_bound=1,
                          inclusive_upper_bound=3, default_value=3)
        if ans == 1:
            print "Aborting Execution"
            sys.exit()
        elif ans == 2:
            print "Recreating det_data_table"
            cursor.execute("DROP TABLE det_data_table")
            cursor.execute(MAKE_DET_DATA_TABLE)
        elif ans == 3:
            print "Skipping writing of det_data_table"
            return
    # now insert the data into the table
    for data in det_data:
        insert_list = generate_insert_list(data, DET_DATA_NAMES)
        cursor.execute(DET_INSERT, insert_list)
    dbcon.commit()


def overwrite_batch_data(batch_data, db_loc):
    """Adds a row to the global batch database using the batch data
    dictionary that was read in earlier

    Parameters
    ----------
    batch_data : dict
        dictionary of information to be dumped into the batch database
    db_loc : str
        path to the batch database file
    """
    # if the database did not already exist it will be created in the connect
    dbcon = sql.connect(db_loc)
    cursor = dbcon.cursor()
    # check if the table exists (in case the db is newly created)
    try:
        cursor.execute(BATCH_TABLE_CMD)
    except sql.OperationalError:
        # if there was an error creating the table then it already exists
        pass
    out_list = generate_insert_list(batch_data, BATCH_DICT_NAMES)
    # move the batch name to the end
    out_list = out_list[1:]
    out_list.append(batch_data["BatchName"])
    # update the entry
    cursor.execute(BATCH_UPDATE, out_list)
    dbcon.commit()
    cursor.execute("VACUUM")
    dbcon.commit()


def add_batch_data(batch_data, db_loc):
    """Adds a row to the global batch database using the batch data
    dictionary that was read in earlier

    Parameters
    ----------
    batch_data : dict
        dictionary of information to be dumped into the batch database
    batch_db_path : str
        path to the batch database file

    Returns
    -------
    success : bool
        True if successful
        False if there was an unrecoverable error
    """
    # if the database did not already exist it will be created in the connect
    dbcon = sql.connect(db_loc)
    cursor = dbcon.cursor()
    # check if the table exists (in case the db is newly created)
    try:
        cursor.execute(BATCH_TABLE_CMD)
    except sql.OperationalError:
        # if there was an error creating the table then it already exists
        pass
    out_list = generate_insert_list(batch_data, BATCH_DICT_NAMES)
    cursor.execute(BATCH_SELECT.format(batch_data["BatchName"]))
    temp = cursor.fetchone()
    if temp is None:
        cursor.execute(BATCH_INSERT, out_list)
        dbcon.commit()
        return True
    else:
        print "Error, batch already in batch database"
        for key, val in zip(BATCH_DICT_NAMES, temp):
            print "%20s:"%key, val
        return False

def generate_insert_list(data, name_list):
    """Generates a list of the contents of the dictionary in the order
    specified by name_list, also does special handling for bools and date times

    Parameters
    ----------
    data : dict
        dictionary of data to be put into a list specially
    name_list : list
        list of dictionary keys in the correct ordering for the insert
    """
    out_list = []
    for key in name_list:
        if "DateTime" in key:
            out_list.append(data[key].__str__())
        elif isinstance(data[key], dt.datetime):
            out_list.append(data[key].__str__())
        elif isinstance(data[key], bool):
            out_list.append(1 if data[key] else 0)
        else:
            out_list.append(data[key])
    return out_list
