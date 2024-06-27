import pandas as pd
import numpy as np
import geopandas as gpd


def interpolate_stoptime(trip_df):
    """
    given a route df with stops and bus location, interpolate the time when the bus is at each stop
    """

    trip_df = trip_df.to_crs("epsg:26971")

    trip_df.loc[:, "data_time"] = pd.to_datetime(
        trip_df["data_time"], format="%Y-%m-%d %H:%M:%S", errors="coerce"
    )

    trip_df["b_value"] = (trip_df["typ"] == "B").cumsum()
    trip_df["b_value"] = trip_df["b_value"].ffill()

    trip_df["s_value"] = (trip_df["typ"] == "S").cumsum()
    trip_df["s_value"] = trip_df["s_value"].ffill()

    # start_loop_end = time.time()

    trip_df["dist_next"] = trip_df["geometry"].distance(trip_df["geometry"].shift(-1))

    # Calculate accumulated distance
    trip_df["accumulated_distance"] = trip_df.groupby("b_value")["dist_next"].cumsum()

    # calculates 'ping_dist' based on 'b_value' groups
    trip_df["ping_dist"] = trip_df.groupby("b_value")["dist_next"].transform("sum")

    # calculates 'stop_dist' based on 's_value' groups
    trip_df["stop_dist"] = trip_df.groupby("s_value")["dist_next"].transform("sum")

    trip_df["stop_sequence"] = trip_df["s_value"]

    ping_times_df = trip_df.loc[
        trip_df.data_time.notna(),
        [
            "seg_combined",
            "stop_sequence",
            "data_time",
            "b_value",
            "typ",
            "unique_trip_vehicle_day",
        ],
    ]
    ping_times_df.loc[:, "ping_time_diff"] = -1 * ping_times_df.data_time.diff(-1)

    # merge the two dataframes to include the 'ping_time_diff' column in trip_df
    trip_df = trip_df.merge(ping_times_df, on="b_value", how="left")

    # replaces NaN values in data_time_y and ping_time_diff for calculation
    trip_df["data_time_y"] = trip_df["data_time_y"].fillna(0)
    trip_df["ping_time_diff"] = trip_df["ping_time_diff"].fillna(0)

    # converts to datetime
    trip_df["data_time_y"] = pd.to_datetime(
        trip_df["data_time_y"], format="%Y-%m-%d %H:%M:%S", errors="coerce"
    )
    trip_df["ping_time_diff"] = pd.to_timedelta(trip_df["ping_time_diff"])

    # calculates times at each bus stop
    stops_df = trip_df.loc[trip_df["typ_x"].isin(["S"])].copy()
    stops_df["original_index"] = stops_df.index
    stops_df.reset_index(drop=True, inplace=True)

    stops_df["bus_stop_time"] = stops_df["data_time_y"] + (
        stops_df["ping_time_diff"]
        * stops_df["accumulated_distance"]
        / stops_df["ping_dist"].replace(0, 0)
    )

    # calculate the speed in meters per second then to mph
    stops_df["time_diff_seconds"] = stops_df["ping_time_diff"].apply(
        lambda x: x.total_seconds()
    )
    stops_df["time_diff_seconds"] = stops_df["time_diff_seconds"].replace(0, 1e-9)
    stops_df["speed_mph"] = (stops_df["ping_dist"] / 1609) / (
        stops_df["time_diff_seconds"] / 3600
    )

    stops_df = stops_df[
        [
            "original_index",
            "seg_combined_x",
            "typ_x",
            "stop_sequence_x",
            "bus_stop_time",
            "time_diff_seconds",
            "stop_dist",
            "speed_mph",
            "unique_trip_vehicle_day_x",
            "stpid",
            "p_stp_id",
        ]
    ]

    new_names = {
        "seg_combined_x": "seg_combined",
        "typ_x": "typ",
        "unique_trip_vehicle_day_x": "unique_trip_vehicle_day",
        "stop_sequence_x": "stop_sequence",
    }

    stops_df.rename(columns=new_names, inplace=True)

    # replace values below 1 or above 115 for speed_mph
    stops_df["speed_mph"] = stops_df["speed_mph"].apply(
        lambda x: np.nan if x < 1 or x > 115 else x
    )
    stops_df["speed_mph"] = stops_df["speed_mph"].fillna(method="ffill")
    stops_df["speed_mph"] = stops_df["speed_mph"].fillna(method="bfill")

    # replace values below 1 for distance
    stops_df["stop_dist"] = stops_df["stop_dist"].apply(
        lambda x: np.nan if x < 1 else x
    )
    stops_df["stop_dist"] = stops_df["stop_dist"].fillna(method="ffill")

    # update time_diff column based on the new time_diff_seconds calculation
    stops_df["time_diff_seconds"] = stops_df["stop_dist"] / (
        stops_df["speed_mph"] / 2.23694
    )

    stops_df.set_index("original_index", inplace=True)
    new_trip_df = pd.concat([stops_df, ping_times_df], axis=0, sort=False)
    new_trip_df.sort_index(inplace=True)
    new_trip_df.reset_index(drop=True, inplace=True)

    # recalculate bus_stop_time for first and last rows
    first_ping_index = new_trip_df.loc[new_trip_df["data_time"].notna()].index[0]
    last_ping_index = new_trip_df.loc[new_trip_df["data_time"].notna()].index[-1]

    new_trip_df.loc[first_ping_index, "bus_stop_time"] = new_trip_df.loc[
        first_ping_index, "data_time"
    ]
    new_trip_df.loc[last_ping_index, "bus_stop_time"] = new_trip_df.loc[
        last_ping_index, "data_time"
    ]

    # first rows
    for i in range(first_ping_index - 1, -1, -1):
        new_trip_df.loc[i, "bus_stop_time"] = new_trip_df.loc[
            i + 1, "bus_stop_time"
        ] - pd.Timedelta(seconds=new_trip_df.loc[i, "time_diff_seconds"])

    # last rows
    for i in range(last_ping_index + 1, len(new_trip_df)):
        new_trip_df.loc[i, "bus_stop_time"] = new_trip_df.loc[
            i - 1, "bus_stop_time"
        ] + pd.Timedelta(seconds=new_trip_df.loc[i, "time_diff_seconds"])

    # clean data table
    new_trip_df = new_trip_df.drop(
        columns=[
            "b_value",
            "ping_time_diff",
            "time_diff_seconds",
            "stop_dist",
            "data_time",
        ]
    )

    new_trip_df = new_trip_df[new_trip_df["typ"] == "S"]

    new_trip_df["bus_stop_time"] = pd.to_datetime(
        new_trip_df["bus_stop_time"], format="%Y-%m-%d %H:%M:%S", errors="coerce"
    )

    return new_trip_df
