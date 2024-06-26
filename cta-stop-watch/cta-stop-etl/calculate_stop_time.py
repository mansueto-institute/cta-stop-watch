import pandas as pd
import warnings
import geopandas as gpd
from geopandas import GeoDataFrame
import pathlib
from shapely import box
import pickle
import os
import logging

from interpolation import interpolate_stoptime

logger_calculate = logging.getLogger(__name__)


warnings.simplefilter(action="ignore", category=FutureWarning)

DIR = pathlib.Path(__file__).parent / "out"


def prepare_segment(pid: str):
    """
    prepares the created segments from a pattern (pid) for use with the bus location.
    """
    # load segment
    # try current and then historic
    segments_gdf = pattern_opener(pid, "segment")

    # .loc
    segments_gdf["prev_segment"] = segments_gdf["segments"]
    segments_gdf["segment"] = segments_gdf["segments"] + 1
    segments_gdf = segments_gdf[["prev_segment", "segment", "geometry"]]

    return segments_gdf


def prepare_trips(pid: str):
    """
    prepare the real trips from a pattern (pid) for use with the segments
    """

    # load trips for a pattern
    trips_df = pd.read_parquet(f"{DIR}/pids/{pid}.parquet")

    trips_gdf = gpd.GeoDataFrame(
        trips_df,
        geometry=gpd.GeoSeries.from_xy(
            x=trips_df.loc[:, "lon"],
            y=trips_df.loc[:, "lat"],
            crs="EPSG:4326",
        ),
    )
    trips_gdf = trips_gdf.sort_values("data_time")
    trips_gdf["bus_location_id"] = trips_gdf.index
    trips_gdf = trips_gdf[
        [
            "rt",
            "pid",
            "bus_location_id",
            "unique_trip_vehicle_day",
            "vid",
            "data_time",
            "geometry",
        ]
    ]

    og_trips_count = trips_gdf["unique_trip_vehicle_day"].nunique()

    # remove trips with all pings in the sameish location
    trips_gdf.to_crs(epsg=26971, inplace=True)
    filtered_trips_gdf = trips_gdf.groupby("unique_trip_vehicle_day").filter(
        lambda x: box(*x.geometry.total_bounds).area > 20
    )

    # keep only last ping of any trips multiple first pings in the same spot
    bad_indexes = []
    for trip_id, trip_gdf in filtered_trips_gdf.groupby("unique_trip_vehicle_day"):
        # if distance is super close
        for i in range(len(trip_gdf) - 1):
            if (
                filtered_trips_gdf.geometry.iloc[i].distance(
                    filtered_trips_gdf.geometry.iloc[i + 1]
                )
                < 5
            ):
                bad_indexes.append(trip_gdf.index[i])
            else:
                break

    filtered_trips_gdf = filtered_trips_gdf[~filtered_trips_gdf.index.isin(bad_indexes)]

    # remove trips with only one ping
    filtered_trips_gdf = filtered_trips_gdf.groupby("unique_trip_vehicle_day").filter(
        lambda x: len(x) > 1
    )

    logging.info(f"Originally {og_trips_count} trips")

    filtered_trips_gdf.to_crs(epsg=4326, inplace=True)

    return (
        filtered_trips_gdf,
        og_trips_count,
    )


def prepare_stops(pid: str):
    """
    Prepares the stops from a pattern (pid) for use with the bus location
    """

    stops_gdf = pattern_opener(pid, "stop")

    stops_gdf.rename(columns={"segment": "seg_combined"}, inplace=True)
    stops_gdf["data_time"] = None

    stops_gdf = stops_gdf[
        ["seg_combined", "typ", "stpid", "p_stp_id", "geometry", "data_time"]
    ]

    stops_gdf.to_crs(epsg=4326, inplace=True)

    return stops_gdf


def merge_segments_trip(trip_gdf, segments_gdf, stops_gdf):
    """
    Confirm bus locations are on route and then create route df with bus location
    """

    # merge the bus locations with the segments to find the segment that the bus is in
    trip_gdf["bus_location"] = trip_gdf.geometry

    # TODO
    # write function to find pings not on route

    processed_trips_gdf = segments_gdf.sjoin(
        trip_gdf, how="inner", predicate="contains"
    )

    processed_trips_gdf["seg_combined"] = (
        processed_trips_gdf["prev_segment"] + processed_trips_gdf["segment"]
    ) / 2

    # determine which segment to put the bus in
    # try first segment it touches
    # if segment already has been assigned after that, try the next one

    last_segment = 0
    assigned_pings = []
    good_indexes = []

    processed_trips_gdf = processed_trips_gdf.sort_values("data_time").reset_index(
        drop=True
    )

    # only overlap segments at end of segment

    # i think itertuples is faster than iterrows.
    for row in processed_trips_gdf.itertuples():
        # if not assigned yet
        if row.bus_location_id not in assigned_pings:
            # if segment trying to assign is not before last segment assigned then assign
            if row.seg_combined > last_segment:
                good_indexes.append(row.Index)

                last_segment = row.seg_combined
                assigned_pings.append(row.bus_location_id)

    processed_trips_gdf = processed_trips_gdf.loc[good_indexes]

    # remove trips with only one ping at this point
    if len(processed_trips_gdf.index) == 1:
        return False

    # merge with stops to get full processed df
    processed_trips_gdf["typ"] = "B"
    processed_trips_gdf = processed_trips_gdf[
        ["seg_combined", "typ", "bus_location", "data_time", "vid"]
    ]
    processed_trips_gdf.rename(columns={"bus_location": "geometry"}, inplace=True)

    final_gdf = pd.concat([processed_trips_gdf, stops_gdf], axis=0)
    final_gdf = final_gdf.reset_index(drop=True)

    final_gdf = final_gdf.sort_values(["seg_combined", "data_time"]).reset_index(
        drop=True
    )
    return final_gdf


def process_one_trip(
    trip_id: str,
    trip_gdf: GeoDataFrame,
    segments_gdf: GeoDataFrame,
    stops_gdf: GeoDataFrame,
):
    """
    process one trip to return a df with the time a bus is at each stop.
    for one trip, only keep points that are on route, then create route df
    with bus location, then interpolate time when bus is at each stop.
    """

    gdf = merge_segments_trip(trip_gdf, segments_gdf, stops_gdf)

    gdf["unique_trip_vehicle_day"] = trip_id

    gdf = interpolate_stoptime(gdf)

    gdf["vid"] = int(trip_gdf[trip_gdf["vid"].notna()]["vid"].unique()[0])
    gdf["rt"] = int(trip_gdf[trip_gdf["rt"].notna()]["rt"].unique()[0])
    gdf["pid"] = int(trip_gdf[trip_gdf["pid"].notna()]["pid"].unique()[0])

    return gdf


def calculate_pattern(pid: str, tester: str = float("inf")):
    """
    Process all the trips for one pattern to return a df with the time a bus is at each stop for every trip.
    """

    # prepare the segments
    segments_gdf = prepare_segment(pid)

    # prepare the trips
    trips_gdf, og_trips_count = prepare_trips(pid)

    # prepare the stops
    stops_gdf = prepare_stops(pid)

    # for each trip in the pattern, create df that has the bus location and the segment that it is in then interpolate
    all_trips = []
    processed_trips_count = 0

    bad_trips = []

    filtered_trips_count = trips_gdf["unique_trip_vehicle_day"].nunique()

    logging.info(
        f"Trying to process {filtered_trips_count} trips for Pattern {pid} after filtering"
    )

    for trip_id, trip_gdf in trips_gdf.groupby("unique_trip_vehicle_day"):
        try:
            processed_trip_df = process_one_trip(
                trip_id, trip_gdf, segments_gdf, stops_gdf
            )
        except Exception as e:
            logging.debug(
                f"Error processing trip {trip_id} for Pattern {pid}. Error: {e}"
            )
            bad_trips.append(trip_id)
            continue

        # put in a dictionary then make a df is much faster
        processed_trip_dict = processed_trip_df.to_dict(orient="records")
        all_trips += processed_trip_dict

        # for testing
        processed_trips_count += 1
        if processed_trips_count >= float(tester):
            break

    logging.info(
        f"Processed {processed_trips_count} trips for Pattern {pid}. There was {len(bad_trips)} trip(s) with errors."
    )

    all_trips_df = pd.DataFrame(all_trips)
    all_trips_df["bus_stop_time"] = pd.to_datetime(all_trips_df["bus_stop_time"])

    # save issue trips examples
    if len(bad_trips) > 0:
        with open(f"{DIR}/qc/bad_trips_{pid}.pickle", "wb") as f:
            # Pickle the 'data' using the highest protocol available.
            pickle.dump(bad_trips, f, pickle.HIGHEST_PROTOCOL)

    return all_trips_df, og_trips_count, processed_trips_count, len(bad_trips)


def calculate_patterns(pids: list):
    """
    calculate stop times for all the patterns
    """

    if not os.path.exists(f"{DIR}/trips"):
        os.makedirs(f"{DIR}/trips")

    all_og_trips_count = 0
    all_processed_trips_count = 0
    all_bad_trips_count = 0

    for pid in pids:
        result, og_trips_count, processed_trips_count, bad_trips_count = (
            calculate_pattern(pid)
        )
        result.to_parquet(f"{DIR}/trips/trips_{pid}_full.parquet", index=False)

        all_og_trips_count += og_trips_count
        all_processed_trips_count += processed_trips_count
        all_bad_trips_count += bad_trips_count

    logging.info(
        f"In total there were {all_og_trips_count} trips, {all_processed_trips_count} were processed, and {all_bad_trips_count} had errors."
    )

    return True


def pattern_opener(pid: str, type: str):
    """
    look for processed pattern date, try current then try historic
    """
    # try current and then historic
    if os.path.exists(f"{DIR}/patterns_current/pid_{pid}_{type}.parquet"):
        gdf = gpd.read_parquet(f"{DIR}/patterns_current/pid_{pid}_{type}.parquet")
    else:
        gdf = gpd.read_parquet(f"{DIR}/patterns_historic/pid_{pid}_{type}.parquet")

    return gdf
