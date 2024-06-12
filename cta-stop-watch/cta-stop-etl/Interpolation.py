import pandas as pd
import numpy as np
import geopandas as gpd


def interpolate_stoptime(trip_df):
    """
    given a route df with stops and bus location, interpolate the time when the bus is at each stop
    """

    b_val = 1
    s_val = 0
    b_indices = []
    s_indices = []
    dist_next = []
    ping_times = []
    bus_stop_times = []
    previous_time = None

    trip_df = trip_df.to_crs("epsg:26971")
    trip_df.loc[:, "data_time"] = pd.to_datetime(trip_df.data_time)

    for i, row in trip_df.iterrows():
        if row["typ"] == "B":
            b_val += 1
        b_indices.append(b_val)
    for i, row in trip_df.iterrows():
        if row["typ"] == "S":
            s_val += 1
        s_indices.append(s_val)

        current_point = row["geometry"]

        next_row = trip_df.iloc[i + 1] if i + 1 < len(trip_df) else None
        next_point = next_row["geometry"] if next_row is not None else None

        # calculates the distance from the current point to the next point
        if next_point is not None:
            distance = current_point.distance(next_point)
        else:
            distance = None
        dist_next.append(distance)

        if row["typ"] == "B":
            if previous_time is not None:
                time_diff = row["data_time"] - previous_time
                ping_times.append(time_diff)
            previous_time = row["data_time"]

    # assigns the 'b_value' and 'dist_next' columns
    trip_df["b_value"] = b_indices
    trip_df["s_value"] = s_indices
    trip_df["dist_next"] = dist_next

    # Calculate accumulated distance
    trip_df["accumulated_distance"] = trip_df.groupby("b_value")["dist_next"].cumsum()

    # calculates 'ping_dist' based on 'b_value' groups
    trip_df["ping_dist"] = trip_df.groupby("b_value")["dist_next"].transform("sum")

    # calculates 'stop_dist' based on 's_value' groups
    trip_df["stop_dist"] = trip_df.groupby("s_value")["dist_next"].transform("sum")

    ping_times_df = trip_df.loc[trip_df.data_time.notna(), ["data_time", "b_value"]]
    ping_times_df.loc[:, "ping_time_diff"] = -1 * ping_times_df.data_time.diff(-1)

    # merge the two dataframes to include the 'ping_time_diff' column in trip_df
    trip_df = trip_df.merge(ping_times_df, on="b_value", how="left")

    # replaces NaN values in data_time_y and ping_time_diff for calculation
    trip_df["data_time_y"] = trip_df["data_time_y"].fillna(0)
    trip_df["ping_time_diff"] = trip_df["ping_time_diff"].fillna(0)

    # converts to datetime
    trip_df["data_time_y"] = pd.to_datetime(trip_df["data_time_y"])
    trip_df["ping_time_diff"] = pd.to_timedelta(trip_df["ping_time_diff"])

    # calculates times at each bus stop
    stops_df = trip_df.loc[trip_df["typ"].isin(["S"])].copy()

    for i, row in stops_df.iterrows():

        proportion = (
            row["accumulated_distance"] / row["ping_dist"]
            if row["ping_dist"] != 0
            else 0
        )

        bus_stop_time = row["data_time_y"] + (row["ping_time_diff"] * proportion)

        bus_stop_times.append(bus_stop_time)

    stops_df["bus_stop_time"] = bus_stop_times

    # calculate the time difference in seconds between consecutive bus stops
    stops_df["time_diff"] = stops_df["bus_stop_time"].diff()
    trip_df = trip_df.merge(stops_df, on="seg_combined", how="left")

    trip_df = trip_df.loc[trip_df["typ_x"].isin(["S"])]
    trip_df["original_index"] = trip_df.index
    trip_df.reset_index(drop=True, inplace=True)

    # find the first valid data_time_y_y that is not 1970-01-01 00:00:00
    # first_valid_idx = trip_df[trip_df['data_time_y_y'] != pd.Timestamp('1970-01-01 00:00:00')].index[0]

    # calculate the speed in meters per second then to mph
    trip_df["time_diff_seconds"] = trip_df["time_diff"].apply(
        lambda x: x.total_seconds()
    )
    trip_df["time_diff_seconds"] = trip_df["time_diff_seconds"].replace(0, 1e-9)
    trip_df["speed_mph"] = (trip_df["stop_dist_y"] / 1609) / (
        trip_df["time_diff_seconds"] / 3600
    )

    trip_df = trip_df[
        [
            "original_index",
            "seg_combined",
            "typ_x",
            "bus_stop_time",
            "time_diff",
            "time_diff_seconds",
            "stop_dist_y",
            "speed_mph",
            "unique_trip_vehicle_day_x",
            "stpid_x",
            "p_stp_id_x",
            "geometry_x",
        ]
    ]
    new_names = {
        "typ_x": "typ",
        "stop_dist_y": "stop_dist",
        "unique_trip_vehicle_day_x": "unique_trip_vehicle_day",
        "stpid_x": "stpid",
        "p_stp_id_x": "p_stp_id",
        "geometry_x": "geometry",
    }

    trip_df.rename(columns=new_names, inplace=True)

    # replace values below 1 for speed_mph
    trip_df["speed_mph"] = trip_df["speed_mph"].apply(
        lambda x: np.nan if x < 1 or x > 100 else x
    )
    trip_df["speed_mph"] = trip_df["speed_mph"].fillna(method="ffill")
    trip_df["speed_mph"] = trip_df["speed_mph"].fillna(method="bfill")

    # replace values below 1 for distance
    trip_df["stop_dist"] = trip_df["stop_dist"].apply(lambda x: np.nan if x < 1 else x)
    trip_df["stop_dist"] = trip_df["stop_dist"].fillna(method="ffill")

    # update time_diff column based on the new time_diff_seconds calculation
    trip_df["time_diff_seconds"] = trip_df["stop_dist"] / (
        trip_df["speed_mph"] / 2.23694
    )

    trip_df.set_index("original_index", inplace=True)
    trip_df = trip_df.merge(
        ping_times_df, left_index=True, right_index=True, how="outer"
    )
    trip_df.reset_index(drop=True, inplace=True)

    # recalculate bus_stop_time for first and last rows
    first_ping_index = trip_df.loc[trip_df["data_time"].notna()].index[0]
    last_ping_index = trip_df.loc[trip_df["data_time"].notna()].index[-1]

    trip_df.loc[first_ping_index, "bus_stop_time"] = trip_df.loc[
        first_ping_index, "data_time"
    ]
    trip_df.loc[last_ping_index, "bus_stop_time"] = trip_df.loc[
        last_ping_index, "data_time"
    ]

    # first rows
    for i in range(first_ping_index - 1, -1, -1):
        trip_df.loc[i, "bus_stop_time"] = trip_df.loc[
            i + 1, "bus_stop_time"
        ] - pd.Timedelta(seconds=trip_df.loc[i, "time_diff_seconds"])

    # last rows
    for i in range(last_ping_index + 1, len(trip_df)):
        trip_df.loc[i, "bus_stop_time"] = trip_df.loc[
            i - 1, "bus_stop_time"
        ] + pd.Timedelta(seconds=trip_df.loc[i, "time_diff_seconds"])

    trip_gdf = gpd.GeoDataFrame(trip_df, geometry="geometry", crs="EPSG:4326")

    return trip_gdf
