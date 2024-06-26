import pandas as pd
import geopandas as gpd
import numpy as np
import os
import pathlib
import re


def create_base_frame(pid):
    """
    Convert raw bus stop level trip data to our base analytics frame
    """

    trips_df = gpd.read_parquet(f"../cta-stop-etl/out/trips/trips_{pid}_full.parquet")

    # Filter out bus locations
    trips_df = trips_df[trips_df["typ"] == "S"]

    # add pid col
    trips_df["pid"] = pid

    # add a hour floor
    trips_df["hour"] = trips_df["bus_stop_time"].dt.floor(freq="h")

    analytics_frame = (
        trips_df.groupby(["hour", "pid", "stpid", "p_stp_id", "geometry"])
        .size()
        .reset_index(name="bus_per_hour")
    )

    analytics_frame["wait_time_minutes"] = 60 / analytics_frame["bus_per_hour"]

    return analytics_frame


def join_metadata(analytics_frame):
    """
    Join in all the metadata for the analytics frame including variables
    for over time analysis (weekdays, weekends, rush hour, month, day of week, month, season, year)
    and location based analysis (community area, census tract, etc)
    """

    DIR = pathlib.Path(__file__).parent
    # Get the metadata
    # rt
    rt_to_pid = pd.read_parquet(f"{DIR}/rt_to_pid.parquet")
    analytics_frame = analytics_frame.merge(rt_to_pid, how="left", on="pid")

    # flags for weekdays, weekends, rush hour, month, day of week, month, season, year
    analytics_frame["year"] = analytics_frame["hour"].dt.year
    analytics_frame["month"] = analytics_frame["hour"].dt.month

    # community area, census tract
    # TODO
    # Join the metadata to the base frame

    return analytics_frame


def create_full_df(pid="all"):

    DIR = pathlib.Path(__file__).parent
    PID_DIR = f"{DIR}/../cta-stop-etl/out/trips"

    pids = []
    for pid_file in os.listdir(PID_DIR):
        numbers = re.findall(r"\d+", pid_file)
        num = numbers[0]
        pids.append(num)

    pids_all = [int(pid) for pid in pids]
    pids_all = set(pids_all)

    rows = []

    if pid != "all":
        pids_all = [pid]

    for pid in pids_all:
        print(f"Processing base frame for {pid}")
        base_df = create_base_frame(pid)
        row = base_df.to_dict(orient="records")
        rows += row

    full_df = gpd.GeoDataFrame(rows, geometry="geometry", crs="EPSG:4326")

    DIR = pathlib.Path(__file__).parent
    if not os.path.exists(f"{DIR}/out"):
        os.makedirs(f"{DIR}/out")

    full_df.to_parquet(f"{DIR}/out/analytics_frame.parquet")

    full_df = join_metadata(full_df)

    return full_df


if __name__ == "__main__":
    full_df = create_full_df()

    DIR = pathlib.Path(__file__).parent

    if not os.path.exists(f"{DIR}/out"):
        os.makedirs(f"{DIR}/out")

    full_df.to_parquet(f"{DIR}/out/analytics_frame.parquet")
