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
    #trip_df['geometry'] = trip_df['location'].astype('str').apply(wkt.loads)
    #print(trip_df)  
    trip_df = trip_df.to_crs(epsg=26971)  

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
            previous_time = row['data_time']
                    
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

    trip_df = trip_df.loc[trip_df['typ'].isin(['S',"B"]), ['unique_trip_vehicle_day', 'seg_combined', 'typ', 'geometry', 'bus_stop_time', 'data_time_x']]


    #convert the timestamp to timedelta
    #trip_df.loc[:,"bus_stop_time"] = pd.to_datetime(trip_df.bus_stop_time)

    return trip_df.to_crs(4326)
