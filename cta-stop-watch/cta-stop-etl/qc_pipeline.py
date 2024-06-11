import os
import pandas as pd
import re
from geopandas import GeoDataFrame
import pathlib
import pickle
import sys


# check for NA or negative times
# check is the first and last stop are the min and max times. Ensure no very long trips (over 4 hours)
# TODO check is avg speed is too high (over 80mph)


# Negative times or NA times
def all_values_check(df: GeoDataFrame):
    """
    return number of trips and actual trips with a negative time or an NA time for each pattern
    """

    # negative_times_df = df[df["time"] < 0]
    na_times_df = df[df["time"].isna()]

    # trips with negative times
    # negative_times_trips = set(
    #     negative_times_df["unique_trip_vehicle_day"].unique().tolist()
    # )

    na_times_trips = set(na_times_df["unique_trip_vehicle_day"].unique().tolist())

    # print(f"There are {len(negative_times_trips)} trips with negative times")
    print(f"There are {len(na_times_trips)} trips with bus stops with NA time")

    # negative_times_trips

    return list(na_times_trips)


def time_issues(df: GeoDataFrame):
    """
    Checks is the first stop is the min time and the last stop is the max time
    Checks if the total trip time is over 4 hours
    """

    for row, (trip_id, trip_gdf) in enumerate(df.groupby("unique_trip_vehicle_day")):
        min_time = pd.Timestamp(trip_gdf["time"].min())
        max_time = pd.Timestamp(trip_gdf["time"].max())

        min_max_time_issue_trips = []
        very_long_trips = []
        same_time_trips = []

        max_hours = 4

        trip_gdf.sort_values("seg_combined", inplace=True)
        # is first stop min
        if (trip_gdf["time"].iloc[0] != min_time) or (
            trip_gdf["time"].iloc[-1] != max_time
        ):
            min_max_time_issue_trips.append(trip_id)

        max_trip_time = f"{max_hours} hours"
        if max_time - min_time < pd.Timedelta(max_trip_time):
            very_long_trips.append(trip_id)

        # check if there are any trips with the same time
        if len(trip_gdf["time"].unique()) != len(trip_gdf["time"]):
            same_time_trips.append(trip_id)

    print(f"There are {len(same_time_trips)} trips with bus stops at the same time")
    print(
        f"There are {len(min_max_time_issue_trips)} trips with the max or min time not the first or last stop"
    )
    print(
        f"There are {len(very_long_trips)} trips with total trip times over {max_trip_time}"
    )

    return same_time_trips, min_max_time_issue_trips, very_long_trips


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


def qc_pipeline(pid: str = "all"):
    """
    This function will run the qc pipeline for the ETL process
    """
    print("Running QC Pipeline")

    DIR = pathlib.Path(__file__).parent / "out"

    qc_rows = []

    issue_examples = []

    if pid == "all":
        pids = os.listdir(f"{DIR}/patterns")
    else:
        pids = [pid]

    for pids in pids:
        pid = re.findall(r"\d+", pids)[0]
        print(f"Running QC for pattern {pid}")

        df = pd.read_parquet(f"{DIR}/trips/pid_{pid}_trips.parquet")

        # TODO add somewhere else
        df["time"] = df[["bus_stop_time", "bus_location_time"]].bfill(axis=1).iloc[:, 0]
        df.drop(columns=["bus_stop_time", "bus_location_time"], inplace=True)
        df = df[(df["typ"] == "S") & (df["time"].notna())]

        # negative_times_trips,
        na_times_trips = all_values_check(df)
        same_time_trips, min_max_time_issue_trips, very_long_trips = time_issues(df)

        negative_times_trips = []
        row = {
            "pid": pid,
            "negative_times_trips": len(negative_times_trips),
            "na_times_trips": len(na_times_trips),
            "same_time_trips": len(same_time_trips),
            "min_max_time_issue_trips": len(min_max_time_issue_trips),
            "very_long_trips": len(very_long_trips),
        }
        qc_rows.append(row)

        # give 5 examples of each issue is available for the pattern
        examples = {
            "pid": pid,
            "negative_times_trips": negative_times_trips[:5],
            "na_times_trips": na_times_trips[:5],
            "same_time_trips": same_time_trips[:5],
            "min_max_time_issue_trips": min_max_time_issue_trips[:5],
            "very_long_trips": very_long_trips[:5],
        }

        issue_examples.append(examples)

    qc_df = pd.DataFrame(qc_rows)

    return qc_df, issue_examples


if __name__ == "__main__":

    DIR = pathlib.Path(__file__).parent / "out"

    if not os.path.exists(f"{DIR}/qc"):
        os.makedirs(f"{DIR}/qc")

    if len(sys.argv) > 1:
        qc_df, issue_examples = qc_pipeline(sys.argv[1])
    else:
        qc_df, issue_examples = qc_pipeline()

    # save issue examples
    with open(f"{DIR}/qc/issue_examples.pickle", "wb") as f:
        # Pickle the 'data' using the highest protocol available.
        pickle.dump(issue_examples, f, pickle.HIGHEST_PROTOCOL)

    # save qc summary df
    qc_df.to_parquet(f"{DIR}/qc/qc_summary_df.parquet")

# to read

# with open("data.pickle", "rb") as f:
#     # The protocol version used is detected automatically, so we do not
#     # have to specify it.
#     data = pickle.load(f)
