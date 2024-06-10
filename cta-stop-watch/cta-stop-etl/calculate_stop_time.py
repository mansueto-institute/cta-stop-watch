import os
import pandas as pd
import numpy as np
import geopandas as gpd
from geopandas import GeoDataFrame
from .Interpolation import interpolate_stoptime
import sys
import re

def prepare_segment(pid: str):
    """
    prepares the created segments from a pattern (pid) for use with the bus location. 
    """
    #load segment
    #gpd.read_parquet
    segments_gdf = gpd.read_parquet(
        f"cta-stop-watch/cta-stop-etl/out/patterns/pid_{pid}_segment.parquet"
    )
    segments_gdf = gpd.read_parquet(
        f"cta-stop-watch/cta-stop-etl/out/patterns/pid_{pid}_segment.parquet"
    )

    segments_gdf["prev_segment"] = segments_gdf["segments"]
    segments_gdf["segment"] = segments_gdf["segments"] + 1
    segments_gdf = segments_gdf[["prev_segment", "segment", "geometry"]]

    return segments_gdf


def prepare_trips(pid: str):
    """
    prepare the real trips from a pattern (pid) for use with the segments
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
    trips_gdf['bus_location_id'] = trips_gdf.index
    trips_gdf = trips_gdf[['bus_location_id',"unique_trip_vehicle_day", "vid", "data_time", "geometry"]]

    # TODO
    # remove trips with only one ping

    return trips_gdf


def prepare_stops(pid: str):
    """
    Prepares the stops from a pattern (pid) for use with the bus location
    """

    # gpd.read_parquet
    stops_gdf = gpd.read_parquet(
        f"cta-stop-watch/cta-stop-etl/out/patterns/pid_{pid}_stop.parquet"
    )

    stops_gdf.rename(
        columns={"segment": "seg_combined"}, inplace=True
    )
    stops_gdf["data_time"] = None
    stops_gdf = stops_gdf[["seg_combined", "typ", "geometry", "data_time"]]

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

    processed_trips_gdf = processed_trips_gdf.sort_values('data_time')

    for index, row in processed_trips_gdf.iterrows():
        #if not assigned yet
        if row['bus_location_id'] not in assigned_pings:
            # if segment trying to assign is not before last segment assigned then assign
            if row["seg_combined"] > last_segment:
                good_indexes.append(index)

                last_segment = row["seg_combined"]
                assigned_pings.append(row['bus_location_id'])

    processed_trips_gdf = processed_trips_gdf.loc[good_indexes]

    # merge with stops to get full processed df

    processed_trips_gdf["typ"] = "B"
    processed_trips_gdf = processed_trips_gdf[
        ["seg_combined", "typ", "bus_location", "data_time"]
    ]
    processed_trips_gdf.rename(columns={"bus_location": "geometry"}, inplace=True)

    final_gdf = pd.concat([processed_trips_gdf, stops_gdf], axis=0)
    final_gdf = final_gdf.sort_values(["seg_combined", "data_time"]).reset_index(
        drop=True
    )

    return final_gdf
   

def process_one_trip(trip_id:str,
    trip_gdf: GeoDataFrame, segments_gdf: GeoDataFrame, stops_gdf: GeoDataFrame
):
    """
    process one trip to return a df with the time a bus is at each stop.
    for one trip, only keep points that are on route, then create route df 
    with bus location, then interpolate time when bus is at each stop. 
    """

    gdf = merge_segments_trip(trip_gdf, segments_gdf, stops_gdf)
    gdf['unique_trip_vehicle_day'] = trip_id

    gdf = interpolate_stoptime(gdf)

    return gdf


def process_pattern(pid: str, tester:str = float("inf")):
    """
    Process all the trips for one pattern to return a df with the time a bus is at each stop for every trip.
    """
    # is data for this pattern available?
    try:
        prepare_segment(pid)
    except NameError:
        print(f"Pattern {pid} not available")
        return False
    
    # prepare the segments
    segments_gdf = prepare_segment(pid)

    # prepare the trips
    trips_gdf = prepare_trips(pid)

    #test 
    #trips_gdf = trips_gdf[trips_gdf["unique_trip_vehicle_day"]== '7295.0235318404101004820292023-01-01']

    # prepare the stops
    stops_gdf = prepare_stops(pid)

    # for each trip in the pattern, create df that has the bus location and the segment that it is in then interpolate
    all_trips = []
    count = 0
    test_flag = False


    print(f"Processing Trips for Pattern {pid}")
    for row, (trip_id, trip_gdf) in enumerate(trips_gdf.groupby("unique_trip_vehicle_day")):
        processed_trip_df = process_one_trip(trip_id,trip_gdf, segments_gdf, stops_gdf)

        # put in a dictionary then make a df is much faster
        processed_trip_dict = processed_trip_df.to_dict(orient='records')
        all_trips += processed_trip_dict

        # for testing
        count += 1
        if count >= float(tester):
            print(f"Processed {count} Trips for Pattern {pid}")
            test_flag = True
            break
    
    if not test_flag:
        print(f"Processed {count} Trips for Pattern {pid}")

    all_trips_gdf = gpd.GeoDataFrame(all_trips, geometry='geometry', crs="EPSG:4326")

    # TODO
    #some clean up that we can get rid of later
    all_trips_gdf["bus_stop_time"] = np.where(all_trips_gdf["bus_stop_time"] == pd.Timedelta("0 days 00:00:00"),  None, all_trips_gdf["bus_stop_time"])
    all_trips_gdf["bus_stop_time"] = pd.to_datetime(all_trips_gdf["bus_stop_time"])

    return all_trips_gdf

def process_all_patterns():
    """
    Process all the trips for all patterns available. Export one file per pattern.
    """
    
    PID_DIR = "out/pids/patterns"
    pids = []
    for pid_file in os.listdir(PID_DIR):
    
        numbers = re.findall(r'\d+', pid_file)
        pid = numbers[0]
        pids.append(pid)

    pids = set(pids)

    if not os.path.exists("cta-stop-watch/cta-stop-etl/out/full_trips"):
        os.makedirs("out/full_trips")

    for pid in pids:
        print(pid)
        result = process_pattern(pid)
        # do something with the result
        result.to_parquet(f'out/full_trips/pid_{pid}_all_trips.to_parquet', index=False)

if __name__ == "__main__":
    """
   adding in a test for one pattern
    """
    if len(sys.argv) > 3:
        print("Usage: python -m cta-stop-watch.cta-stop-etl.calculate_stop_time <[optional] pid> <[optional] number of trips to process>")
        sys.exit(1)
    elif len(sys.argv) == 3:
        # run in testing model with limited number of trips
        result = process_pattern(sys.argv[1], sys.argv[2])
        result.to_parquet('test_full_pattern.parquet', index=False)
    elif len(sys.argv) == 2:
        # run for pattern for all trips
        result = process_pattern(sys.argv[1])
        result.to_parquet('test_full_pattern.parquet', index=False)
    elif len(sys.argv) == 1:
        # run for all patterns and all trips
        process_all_patterns()
        print("done")

