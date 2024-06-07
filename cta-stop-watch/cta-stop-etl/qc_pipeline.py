import os
import pandas as pd
import re


def stops_per_pattern():
    """
    return a df of the number of bus stops per pattern
    """

    PID_DIR = "out/patterns"
    pids = []
    for pid_file in os.listdir(PID_DIR):
        numbers = re.findall(r"\d+", pid_file)
        pid = numbers[0]
        pids.append(pid)

    pids = set(pids)

    all_pids = []
    for pid in pids:
        # print(pid)
        stops_df = pd.read_parquet(f"out/patterns/pid_{pid}_stop.parquet")
        stops = len(stops_df[stops_df["typ"] == "S"].index)
        dict = {"pid": pid, "stops": stops}
        all_pids.append(dict)

    all_pids_df = pd.DataFrame(all_pids)

    return all_pids_df


def qc_pipeline():
    """
    This function will run the qc pipeline for the ETL process
    """
    print("Running QC Pipeline")
