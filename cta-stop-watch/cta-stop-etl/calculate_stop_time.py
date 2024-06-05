import os
import pandas as pd
import geopandas as gpd
from geopandas import GeoDataFrame
import sys
from shapely import wkt

## CONSTANTS
M_TO_FT = 3.280839895
BUFFER_DIST = 50.0


def prepare_segment(pid: str):
    """
    prepares the segments for a pattern
    """
    # load segment
    segments_df = pd.read_parquet(
        f"cta-stop-watch/cta-stop-etl/out/patterns/pid_{pid}_segment.parquet"
    )

    segments_df["geometry"] = segments_df["geometry"].apply(wkt.loads)
    segments_gdf = gpd.GeoDataFrame(segments_df, crs="epsg:4326")
    segments_gdf["prev_segment"] = segments_gdf["segments"]
    segments_gdf["segment"] = segments_gdf["segments"] + 1
    segments_gdf = segments_gdf[["prev_segment", "segment", "geometry"]]

    return segments_gdf


def prepare_trips(pid: str):
    """
    prepare the real trips for pattern
    """

    # load trips for a pattern
    trips_df = pd.read_parquet(f"cta-stop-watch/cta-stop-etl/out/pids/{pid}.parquet")

    trips_gdf = gpd.GeoDataFrame(
        trips_df,
        geometry=gpd.GeoSeries.from_xy(
            x=trips_df.loc[:, "lon"],
            y=trips_df.loc[:, "lat"],
            crs="EPSG:4326",
        ),
    )
    trips_gdf = trips_gdf.sort_values("data_time")
    trips_gdf = trips_gdf[["unique_trip_vehicle_day", "vid", "data_time", "geometry"]]

    return trips_gdf


def prepare_stops(pid: str):

    stops_df = pd.read_parquet(
        f"cta-stop-watch/cta-stop-etl/out/patterns/pid_{pid}_stop.parquet"
    )

    stops_df.rename(
        columns={"segment": "seg_combined", "geometry": "location"}, inplace=True
    )
    stops_df["data_time"] = None
    stops_df = stops_df[["seg_combined", "typ", "location", "data_time"]]

    return stops_df


def process_one_trip(trip_id:str,
    trip_df: GeoDataFrame, segments_df: GeoDataFrame, stops_df: GeoDataFrame
):
    """
    for one trip, merge the real pings with the segments
    """

    df = merge_segments_trip(trip_df, segments_df, stops_df)
    df['unique_trip_vehicle_day'] = trip_id

    # TODO
    # df = interpolate_stoptime(df)

    return df


def merge_segments_trip(trip_gdf, segments_gdf, stops_df):
    """
    create full trip data frame from bus locations and segment stops
    """

    # merge the bus locations with the segments to find the segment that the bus is in
    trip_gdf["bus_location"] = trip_gdf.geometry

    processed_trips_gdf = segments_gdf.sjoin(
        trip_gdf, how="inner", predicate="contains"
    )

    processed_trips_gdf["seg_combined"] = (
        processed_trips_gdf["prev_segment"] + processed_trips_gdf["segment"]
    ) / 2

    # merge with stops to get full processed df

    processed_trips_gdf["typ"] = "B"
    processed_trips_gdf.rename(columns={"bus_location": "location"}, inplace=True)
    processed_trips_gdf = processed_trips_gdf[
        ["seg_combined", "typ", "location", "data_time"]
    ]

    final_df = pd.concat([processed_trips_gdf, stops_df], axis=0)
    final_df = final_df.sort_values(["seg_combined", "data_time"]).reset_index(
        drop=True
    )

    return final_df


def interpolate_stoptime(trip_df, bus_location_index: str):

    # TODO
    pass


def process_pattern(pid: str, tester:int = float("inf")):
    """
    take real time bus data and merge with the segments dataframe.
    each row will be a bus location and the segment that it is in.
    """
    # is pattern available from the api?
    # TODO

    # prepare the segments
    segments_gdf = prepare_segment(pid)

    # prepare the trips
    trips_gdf = prepare_trips(pid)

    # prepare the stops
    stops_df = prepare_stops(pid)

    # for each trip in the pattern, create df that has the bus location and the segment that it is in then interpolate
    all_trips_gdf = gpd.GeoDataFrame()
    count = 0
    for row, (trip_id, trip_gdf) in enumerate(trips_gdf.groupby("unique_trip_vehicle_day")):
        print(trip_id)
        processed_trip_df = process_one_trip(trip_id,trip_gdf, segments_gdf, stops_df)

        all_trips_gdf = pd.concat([all_trips_gdf, processed_trip_df], axis=0).reset_index(drop=True)
        count += 1
        if count >= int(tester):
            break

    return all_trips_gdf

def process_all_patterns():
    """
    run everything

    """

    # TODO 
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
        """

if __name__ == "__main__":
    """
   adding in a test for one pattern
    """
    if sys.argv[1] == 'process_pattern' and len(sys.argv) != 3:
        print("Usage: python -m process_pattern <pid> <[optional] number of trips to process>")
        sys.exit(1)
    elif sys.argv[1] == 'process_pattern':
        result = process_pattern(sys.argv[2], sys.argv[3])
        print(result.shape)
        result.to_csv('test_full_pattern.csv', index=False)
    
    if sys.argv[1] == 'process_all_patterns':
        process_all_patterns()
        print("done")

