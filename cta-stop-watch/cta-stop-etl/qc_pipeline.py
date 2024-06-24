import os
import pandas as pd
import re
from geopandas import GeoDataFrame
import pathlib
import pickle
import sys


# check for NA or negative times
# check is the first and last stop are the min and max times. Ensure no very long trips (over 4 hours)
# check is avg speed is too high (over 80mph)


# NA times and avg sped too high
def all_values_check(df: GeoDataFrame):
    """
    return number of trips and actual trips with a bus stop with NA time or a max speed over 150mph for a pattern
    """

    # Avg speed too high
    avg_speed_trips_df = df[df["speed_mph"] > 150]
    avg_speed_trips = set(
        avg_speed_trips_df["unique_trip_vehicle_day"].unique().tolist()
    )

    na_times_df = df[df["bus_stop_time"].isna()]
    na_times_trips = set(na_times_df["unique_trip_vehicle_day"].unique().tolist())

    print(f"There are {len(na_times_trips)} trips with bus stops with NA time")
    print(f"There are {len(avg_speed_trips)} trips with bus speeds over 150mph")

    # negative_times_trips

    return list(na_times_trips), list(avg_speed_trips)


def time_issues(df: GeoDataFrame):
    """
    Checks is the first stop is the min time and the last stop is the max time
    Checks if the total trip time is over 8 hours
    """
    min_max_time_issue_trips = []
    very_long_trips = []
    same_time_trips = []

    for row, (trip_id, trip_gdf) in enumerate(df.groupby("unique_trip_vehicle_day")):
        min_time = pd.Timestamp(trip_gdf["bus_stop_time"].min())
        max_time = pd.Timestamp(trip_gdf["bus_stop_time"].max())

        max_hours = 8

        trip_gdf.sort_values("seg_combined", inplace=True)
        # is first stop min
        if (trip_gdf["bus_stop_time"].iloc[0] != min_time) or (
            trip_gdf["bus_stop_time"].iloc[-1] != max_time
        ):
            min_max_time_issue_trips.append(trip_id)

        max_trip_time = f"{max_hours} hours"

        if (max_time - min_time) > pd.Timedelta(max_trip_time):
            very_long_trips.append(trip_id)

        # check if there are any trips with the same time
        if len(trip_gdf["bus_stop_time"].unique()) != len(trip_gdf["bus_stop_time"]):
            same_time_trips.append(trip_id)

    print(f"There are {len(same_time_trips)} trips with bus stops at the same time")
    print(
        f"There are {len(min_max_time_issue_trips)} trips with the max or min time not the first or last stop"
    )
    print(
        f"There are {len(very_long_trips)} trips with total trip times over {max_trip_time}"
    )

    return same_time_trips, min_max_time_issue_trips, very_long_trips


# TODO
# Variations for bus stop


def qc_pipeline(pids: list):
    """
    This function will run the qc pipeline for the ETL process
    """

    qc_rows = []

    issue_examples = []

    DIR = pathlib.Path(__file__).parent / "out"

    for pid in pids:
        print(f"Running QC for pattern {pid}")

        try:
            df = pd.read_parquet(f"{DIR}/trips/trips_{pid}_full.parquet")
        except FileNotFoundError:
            print(f"Trips for pattern {pid} not available")
            continue

        df = df[df["typ"] == "S"]

        na_times_trips, avg_speed_trips = all_values_check(df)
        same_time_trips, min_max_time_issue_trips, very_long_trips = time_issues(df)

        row = {
            "pid": pid,
            "avg_speed_trips": len(avg_speed_trips),
            "na_times_trips": len(na_times_trips),
            "same_time_trips": len(same_time_trips),
            "min_max_time_issue_trips": len(min_max_time_issue_trips),
            "very_long_trips": len(very_long_trips),
        }
        qc_rows.append(row)

        # give 5 examples of each issue is available for the pattern
        examples = {
            "pid": pid,
            "avg_speed_trips": avg_speed_trips[:5],
            "na_times_trips": na_times_trips[:5],
            "same_time_trips": same_time_trips[:5],
            "min_max_time_issue_trips": min_max_time_issue_trips[:5],
            "very_long_trips": very_long_trips[:5],
        }

        issue_examples.append(examples)

    qc_df = pd.DataFrame(qc_rows)

    DIR = pathlib.Path(__file__).parent / "out"

    # save issue examples
    with open(f"{DIR}/qc/issue_examples.pickle", "wb") as f:
        # Pickle the 'data' using the highest protocol available.
        pickle.dump(issue_examples, f, pickle.HIGHEST_PROTOCOL)

    # save qc summary df
    qc_df.to_parquet(f"{DIR}/qc/qc_summary_df.parquet")

    return True


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
