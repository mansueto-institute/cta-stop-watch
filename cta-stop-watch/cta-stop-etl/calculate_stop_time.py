import os
import pandas as pd
import geopandas as gpd
from geopandas import GeoDataFrame
import sys
from shapely import wkt
import re

## CONSTANTS
M_TO_FT = 3.280839895


def prepare_segment(pid: str):
    """
    prepares the created segments from a pattern (pid) for use with the bus location. 
    """
    #load segment
    #gpd.read_parquet
    segments_df = pd.read_parquet(
        f"cta-stop-watch/cta-stop-etl/out/patterns/pid_{pid}_segment.parquet"
    )
    segments_df = pd.read_parquet(
        f"cta-stop-watch/cta-stop-etl/out/patterns/pid_{pid}_segment.parquet"
    )

    # this will go away
    segments_df["geometry"] = segments_df["geometry"].apply(wkt.loads)
    segments_gdf = gpd.GeoDataFrame(segments_df, crs="epsg:4326")


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
    trips_gdf = trips_gdf[["unique_trip_vehicle_day", "vid", "data_time", "geometry"]]

    return trips_gdf


def prepare_stops(pid: str):
    """
    Prepares the stops from a pattern (pid) for use with the bus location
    """

    # gpd.read_parquet
    stops_df = pd.read_parquet(
        f"cta-stop-watch/cta-stop-etl/out/patterns/pid_{pid}_stop.parquet"
    )

    stops_df.rename(
        columns={"segment": "seg_combined", "geometry": "location"}, inplace=True
    )
    stops_df["data_time"] = None
    stops_df = stops_df[["seg_combined", "typ", "location", "data_time"]]

    return stops_df


def merge_segments_trip(trip_gdf, segments_gdf, stops_df):
    """
    Confirm bus locations are on route and then create route df with bus location
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

def interpolate_stoptime(trip_df):
    """
    given a route df with stops and bus location, interpolate the time when the bus is at each stop
    """ 
    b_val = 1 
    b_indices = [] 
    dist_next = [] 
    ping_times = []
    previous_time = None
    
    #creates 'geometry' column for wkt and crs converstion
    # this should go away
    trip_df['geometry'] = trip_df['location'].astype('str').apply(wkt.loads)
    trip_df = gpd.GeoDataFrame(trip_df, geometry='geometry', crs='epsg:26971')


    trip_df.loc[:,"data_time"] = pd.to_datetime(trip_df.data_time)
    
    for i, row in trip_df.iterrows():
        if row['typ'] == 'B':
            b_val += 1
        b_indices.append(b_val)
    
        current_point = row['geometry']

        next_row = trip_df.iloc[i + 1] if i + 1 < len(trip_df) else None
        next_point = next_row['geometry'] if next_row is not None else None
        
        #calculates the distance from the current point to the next point
        if next_point is not None:
            distance = current_point.distance(next_point)
        else:
            distance = None
        
        dist_next.append(distance)

        if row['typ'] == 'B':
            if previous_time is not None:
                time_diff = row['data_time'] - previous_time
                ping_times.append(time_diff)
            previos_time = row['data_time']
                    
    #assigns the 'b_value' column 
    trip_df['b_value'] = b_indices
    
    #add the 'dist_next' column 
    trip_df['dist_next'] = dist_next

    #Calculate accumulated distance
    trip_df["accumulated_distance"] = trip_df.groupby("b_value")["dist_next"].cumsum()
    
    #calculates 'ping_dist' based on 'b_value' groups
    trip_df['ping_dist'] = trip_df.groupby('b_value')['dist_next'].transform('sum')

    ping_times_df = trip_df.loc[trip_df.data_time.notna(),["data_time", "b_value"]]
    ping_times_df.loc[:,"ping_time_diff"] = -1*ping_times_df.data_time.diff(-1)
    #replaces NaN values in ping_time_diff with zero
    ping_times_df['ping_time_diff'] = ping_times_df['ping_time_diff'].fillna(pd.Timedelta(seconds=0))

    #merge the two dataframes to include the 'ping_time_diff' column in trip_df
    trip_df = trip_df.merge(ping_times_df, on='b_value', how='left')

    #replaces NaN values in data_time_y and ping_time_diff for calculation
    trip_df['data_time_y'] = trip_df['data_time_y'].fillna(pd.Timedelta(seconds=0))
    trip_df['ping_time_diff'] = trip_df['ping_time_diff'].fillna(pd.Timedelta(seconds=0))

    #calculates times at each bus stop 
    for i, row in trip_df.iterrows():
        if row["typ"] == "S":
            ping_dist = row["ping_dist"]
            accumulated_distance = row["accumulated_distance"]
            ping_time_diff = row["ping_time_diff"]
        
            proportion = accumulated_distance / ping_dist if ping_dist != 0 else 0
    
            bus_stop_time = row["data_time_y"] + (ping_time_diff * proportion)
            
            #update the bus_stop_time in the dataframe
            trip_df.at[i, "bus_stop_time"] = bus_stop_time

    trip_df = trip_df.loc[trip_df['typ'].isin(['S','B']), ['unique_trip_vehicle_day', 'seg_combined', 'typ', 'location', 'bus_stop_time']]

    #convert the timestamp to timedelta
    #trip_df.loc[:,"bus_stop_time"] = pd.to_datetime(trip_df.bus_stop_time)
    
    return trip_df
    

def process_one_trip(trip_id:str,
    trip_df: GeoDataFrame, segments_df: GeoDataFrame, stops_df: GeoDataFrame
):
    """
    process one trip to return a df with the time a bus is at each stop.
    for one trip, only keep points that are on route, then create route df 
    with bus location, then interpolate time when bus is at each stop. 
    """

    df = merge_segments_trip(trip_df, segments_df, stops_df)
    df['unique_trip_vehicle_day'] = trip_id

    df = interpolate_stoptime(df)

    return df



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

    # prepare the stops
    stops_df = prepare_stops(pid)

    # for each trip in the pattern, create df that has the bus location and the segment that it is in then interpolate
    all_trips = []
    count = 0
    for row, (trip_id, trip_gdf) in enumerate(trips_gdf.groupby("unique_trip_vehicle_day")):
        print(trip_id)
        processed_trip_df = process_one_trip(trip_id,trip_gdf, segments_gdf, stops_df)

        # put in a dictionary then make a df is much faster
        processed_trip_dict = processed_trip_df.to_dict(orient='records')
        all_trips += processed_trip_dict

        # for testing
        count += 1
        if count >= float(tester):
            break


    all_trips_df = pd.DataFrame(all_trips)

    return all_trips_df

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
    for pid in pids:
        print(pid)
        result = process_pattern(pid)
        # do something with the result
        #result.to_csv(f'out/full_trips/pid_{pid}_all_trips.csv', index=False)

    

if __name__ == "__main__":
    """
   adding in a test for one pattern
    """
    if len(sys.argv) > 3:
        print("Usage: python -m cta-stop-watch.cta-stop-etl.calculate_stop_time <pid> <[optional] number of trips to process>")
        sys.exit(1)
    elif len(sys.argv) == 3:
        # run in testing model with limited number of trips
        result = process_pattern(sys.argv[1], sys.argv[2])
        result.to_csv('test_full_pattern.csv', index=False)
    elif len(sys.argv) == 2:
        # run for pattern for all trips
        result = process_pattern(sys.argv[1])
        result.to_csv('test_full_pattern.csv', index=False)
    elif len(sys.argv) == 1:
        # run for all patterns and all trips
        process_all_patterns()
        print("done")

