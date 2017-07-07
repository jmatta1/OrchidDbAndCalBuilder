"""Functions to create, update, and add to the various databases"""
import sqlite3 as sql
import os

BATCH_TABLE_CMD = """CREATE TABLE batch_table (
    batch_name text PRIMARY KEY,
    array_x real NOT NULL,
    array_y real NOT NULL,
    raw_root_location text NOT NULL,
    run_data_location text NOT NULL,
    det_data_location text NOT NULL,
    tree_gen int NOT NULL,
    tree_root_location text NOT NULL,
    hist_integration_time real NOT NULL,
    start_us_epoch int NOT NULL,
    start_time text NOT NULL,
    stop_us_epoch int NOT NULL,
    stop_time text NOT NULL,
    run_count int NOT NULL,
    det_count int NOT NULL,
    first_buffer_skip int NOT NULL,
    has_been_calibrated int DEFAULT 0,
    has_been_decomposed int DEFAULT 0,
    proc_root_location text DEFAULT '',
    run_db_location text DEFAULT '',
    det_db_location text DEFAULT ''
);
"""

DICT_NAMES = ["BatchName",          "ArrayX",           "ArrayY",
              "RootFileLocation",   "RunDataLocation",  "DetDataLocation",
              "TreeGenerated",      "TreeFileLocation", "IntTime",
              "StartEpochMicroSec", "StartDateTime",    "StopEpochMicroSec",
              "StopDateTime",       "RunCount",         "DetCount",
              "FirstBufferSkipped", "IsCalibrated",     "IsDecomposed",
              "ProcRootLoc",        "RunDbLoc",         "DetDbLoc"]


BATCH_INSERT = "INSERT INTO batch_table VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"


def add_batch_data(batch_data, db_loc):
    """Adds a row to the global batch database using the batch data
    dictionary that was read

    PARAMETERS
    ----------
    batch_data : dict
        dictionary of information to be dumped into the batch database

    RETURN
    ------
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
    except sql.OperationalError as err:
        # if there was an error creating the table then it already exists
        pass
    # add the keys to the dictionary containing the extra column values
    batch_data["IsCalibrated"] = False
    batch_data["IsDecomposed"] = False
    batch_data["ProcRootLoc"] = ""
    batch_data["RunDbLoc"] = ""
    batch_data["DetDbLoc"] = ""
    out_list = []
    for key in DICT_NAMES:
        if "DateTime" in key:
            out_list.append(batch_data[key].__str__())
        elif type(batch_data[key]) is bool:
            out_list.append(1 if batch_data[key] else 0)
        else:
            out_list.append(batch_data[key])
    test_cmd = "SELECT * FROM batch_table WHERE batch_name='{0:s}';".format(batch_data["BatchName"])
    cursor.execute(test_cmd)
    temp = cursor.fetchone()
    if temp is None:
        cursor.execute(BATCH_INSERT, out_list)
        dbcon.commit()
        return True
    else:
        print "Error, batch already in batch database"
        for key, val in zip(DICT_NAMES, temp):
            print "%20s:"%key, val
        return False

