import os
import pandas as pd
import geopandas as gpd
from geopandas import GeoDataFrame, GeoSeries
from shapely import LineString
from retrieve_patterns import save_pattern_api

## CONSTANTS
M_TO_FT = 3.280839895
BUFFER_DIST = 50.0


def process_pattern(pid: str, pid_df: GeoDataFrame):
    """
    take real time bus data and merge with the segments dataframe.
    each row will be a bus location and the segment that it is in.
    """
    # is pattern available from the api?
    if not save_pattern_api(pid):
        print(f"skipping {pid=}")
        print(pid_df.iloc[-1].tmstmp)
        return False

    # grab it is it is
    segments_df = gpd.read_parquet(f"out/pattern/pid_{pid}_segment.parquet")

    # for each trip in the pattern, create df that has the bus location and the segment that it is in then interpolate
    all_trips_df = pd.DataFrame()
    for row, (trip_id, tripdf) in pid_df.groupby("unique_trip_vehicle_day"):
        print(trip_id)
        df = one_trip_combine(tripdf, segments_df)
        final_df = interpolate_stoptime(df)
        all_trips_df = pd.concat([all_trips_df, final_df], axis=0)

    return all_trips_df


def one_trip_combine(trip_df: GeoDataFrame, segments_df: GeoDataFrame):
    """
    for one trip, merge the real pings with the segments
    """

    # row per ping with what segment it was in
    # currently removes pings that are not in a segment
    merged_df = segments_df.sjoin(trip_df, how="inner", predicate="within")

    # create what segment it was between
    merged_df["seq_combined"] = merged_df["prev_seq"] + merged_df["seq"] / 2

    merged_df = merged_df[["seq_combined", "bus_timestamp", "bus_location"]].copy()

    # add in the actual stops
    # create stops df to add in
    stops_df = segments_df[["seq", "geometry"]].copy()
    stops_df.rename(
        {"seq": "seq_combined", "geometry": "stop_location"}, axis=1, inplace=True
    )
    stops_df["bus_timestamp"] = None
    stops_df["bus_location"] = None

    # concat the stops with the bus locations
    final_df = pd.concat([merged_df, stops_df], axis=0)

    final_df.sort_values(
        ["seq_combined", "bus_timestamp"], ascending=[True, True], inplace=True
    )

    return final_df


def interpolate_stoptime(trip_df, bus_location_index: str):

    # for each bus location create segments until another bus location
    bus_stop_distances = []
    segment_dist = 0
    for row in trip_df[bus_location_index + 1]:
        # dist between
        segment_dist += row["bus_location"].distance(trip_df[bus_location_index - 1])
        # find length between each segment
        # if hit a bus stop, store distance
        if row["is_stop"]:
            bus_stop_distances.append("segment_dist")

    distance_between_pings = segment_dist.sum()

    # time for bus stop is trip_df.iloc[bus_location_index]["bus_timestamp"] * (bus_stop_distances/distance_between_pings)


def process_all_patterns():
    """
    run everything

    """
    PID_DIR = "out/pids"
    for pid_file in os.listdir(PID_DIR):
        # print(f"{PID_DIR}/{pid_file}")
        pid_df = pd.read_parquet(f"{PID_DIR}/{pid_file}")
        pid_df.loc[:, "tmstmp"] = pd.to_datetime(
            pid_df.loc[:, "tmstmp"], format="%Y%m%d %H:%M"
        )
        pid_df = gpd.GeoDataFrame(
            pid_df,
            geometry=gpd.GeoSeries.from_xy(
                x=pid_df.loc[:, "lon"], y=pid_df.loc[:, "lat"], crs="EPSG:4326"
            ),
        )
        pid = pid_file.replace(".0.parquet", "")
        print(pid_df)
        if process_pattern(pid, pid_df):
            break


if __name__ == "__main__":
    process_all_patterns()
