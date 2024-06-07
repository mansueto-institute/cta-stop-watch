import os
import pandas as pd
import re


# Negative times
def negative_time_check(pid: str):
    """
    return number of trips and actual trips with a negative time
    """

    all_trips_df = pd.read_parquet(f"out/trips/trips_{pid}.parquet")

    negative_times_df = all_trips_df[all_trips_df["time"] < 0]

    # trips with negative times
    negative_times_trips = set(
        negative_times_df["unique_trip_vehicle_day"].unique().tolist()
    )

    return len(negative_times_trips), negative_times_trips


# Avg speed too high


# Variations for bus stop


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
