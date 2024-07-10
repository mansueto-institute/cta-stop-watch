
# Script logic  
# For bus stops 
    # For every pid that goes throug it
        # For different time budgets
            # Convert segment into shape 

# TODO
# Update docstrings and typing 
# Make function for labeling time variable 
# Address edge cases (stop is start/end of path)
# Dissolve shapes grouping by bus stop and time budget *** tried 
# Add transfers to simulation 
# Add initial wait based on actual waiting and delay times
# Add walking time from final bus stop within time budget

# Tracke all skipped PIDs

# Run code for the whole data set: ask for stops actual metrics with stop sequence


# IMPORTS ---------------------------------------------------------------------

import polars as pl 
import pandas as pd
import geopandas as gpd
import pathlib
import logging
import time
import datetime

# Import from module with "-" on its name 
import importlib  
ppatt = importlib.import_module("cta-stop-watch.cta-stop-etl.process_patterns")


# LOGGER ----------------------------------------------------------------------

# Start logger for this script
logger = logging.getLogger(__name__)

logging.basicConfig(
    filename="prepare_data.log",
    filemode="w",
    # format = "%(levelname)-8s [%(filename)s:%(lineno)d] %(message)s",
    encoding="utf-8",
    level=logging.DEBUG,
    # level=logging.INFO
)

start_tmstmp = time.time()
start_string = time.asctime(time.localtime())
logging.info(f"CTA BUSES ETL PIPELINE STARTED AT: {start_string}")
logging.info(f"{'-'*69}")

# CONTANTS --------------------------------------------------------------------

# Paths 
DIR = pathlib.Path(__file__) 
DIR_INP = DIR.parents[2] / "cta-stop-etl/out/"
DIR_PID = pathlib.Path(__file__).parents[2] / "cta-stop-etl/out/"


# Simulation parameter: Time
TIME_WINDOWS = [5, 10, 15, 30, 60, 90, 120]
# TIME_WINDOWS = [2, 5, 10, 15]
TIME_WINDOWS = [datetime.timedelta(minutes=t) for t in TIME_WINDOWS]

# Simulation parameter: Tranfers
NUM_TRANSFERS = 0

# Data 


# FUNCTIONS -------------------------------------------------------------------


def prepare_input_dataset():

    logging.info("Merging infro with bus sequence")

    # Dictionary for bus stops id, bus stop sequence, and pattern id
    df_stop_seq = pl.read_parquet("df_stop_pid_seq.parquet").with_columns(
        # Cast types so that ids are the same in both data sets
        pl.col("pid").cast(pl.Int16).cast(pl.String),
        pl.col("stop_id").cast(pl.Int16).cast(pl.String)
    ).unique()

    # Stop sequence info: filter for relevant time period 
    df_stops_metrics = pl.read_parquet("actual_stop_metrics_df.parquet")

    df_stops_metrics = df_stops_metrics.filter(
        pl.col("period_value") == 2024
    ).with_columns(
        # Cast types so that ids are the same in both data sets
        pl.col("pid").cast(pl.Int16).cast(pl.String),
        pl.col("stop_id").cast(pl.Int16).cast(pl.String)
    )
    
    # Join actual metrics with sequence order of bus stops
    df_stops_metrics = df_stops_metrics.join(
        df_stop_seq,
        how = "left",
        on = ["stop_id", "pid"],
        coalesce=True
    )

    return df_stops_metrics



def compute_travel_time_baseline(df_catalogue: pl.DataFrame, df_stops_metrics: pl.DataFrame, stop: str, pid: str) -> pl.DataType:
    """ 
    Takes a pair of a stop and a pattern and compute the travel time it would 
    take to go from that stop to the end of that pattern. Notice that the stop
    is not necessarily the starting point of the pattern.

    Inputs: 
        - df_catalogue (pl.DataFrame): A dataframe with valid pairs of stops 
        and pids, with information of the route the pair corresponds to.
        - stop (str): a stop id where the travel starts 
        - pid (str): a pattern id that goes through that stop 
        - initial_wait (timedelta): Any metric used to reflect the time a user 
        waits at the bus stop for the bus to arrive. 

    Returns: 
        - df_travel_time (pl.Dataframe): A dataframe with the pid segment from 
        the bus stop to the end of the pattern that contains the cumulative 
        travel time elapsed at different bus stops. 
    """
    
    # Get route when bus stop is present in the catalogue 
    try:
        df_stop_pid_pair = df_catalogue.filter(
            (pl.col("stop_id") == stop) & (pl.col("pid") == pid))
        route = df_stop_pid_pair["route_id"].item()
    except (KeyError, ValueError) as e: 
        logging.debug("Stop/pid pair not in catalogue")
        logging.error(e)
        route = None

    # Filter bus stop' pattern info for corresponding time period a
    df_pid = df_stops_metrics.filter(
        (pl.col("pid") == pid) & 
        (pl.col("period") == "year") & 
        (pl.col("period_value") == 2024) &
        True
                                    ).with_columns(
                                        pl.col("stop_sequence").cast(pl.Int16)
                                    ).sort(pl.col("stop_sequence")
                                    ).rename({"median_actual_time_to_previous_stop": "time_to_previous"}
                                    ).select(["rt", "pid", "stop_sequence", "stop_id", "period_value", "time_to_previous"]
        )

    
    df_start_stop = df_pid.with_row_index().filter(pl.col("stop_id") == stop)

    # Some STOP/PID patterns will not be available for the time period
    if df_pid.shape[0] == 0:
        logging.error(f"PID {pid} not present in stop metrics")
        return None
    elif df_start_stop.shape[0] == 0:
        logging.debug(f"Bus stop {stop} not present in stop metrics for 2024")
        return None
    elif df_start_stop.shape[0]  > 1: 
        logging.info("More than one observation found for the stop")
        logging.debug(f"{df_start_stop = }")

        # Remove duplicates where time is the same
        df_start_stop = df_start_stop.unique(subset=["time_to_previous"])
        logging.info("After filtering for duplicate time values:")
        logging.debug(f"{df_start_stop = }")

                               
    start = df_start_stop["index"].item()
    end = len(df_pid)
    df_remaining_path = df_pid.slice(start, end)
    
    # TODO: Decide what to do with single observation paths and 
    # Edge cases (bus stop is the start or the end of the pid)
    if len(df_remaining_path) == 1: 
        pass

    # Compute total travel time with bus stop as start of the travel 
    # Note: Be careful handling time operations
    start_time = df_start_stop["time_to_previous"].item()
    initial_wait = datetime.timedelta(0)

    df_travel_time = df_remaining_path.with_columns(
        travel_time_elapsed = pl.cum_sum("time_to_previous") - start_time + initial_wait, 
        route = pl.lit(route)
    )

    return df_travel_time


def find_transfers(): 
    pass

def add_walking_distance(): 
    pass

def find_reachable_segment_by_time(df_travel_time: pl.DataFrame, df_pattern: pl.DataFrame, stop: str, pid: str, time_budget: datetime.timedelta) -> gpd.GeoDataFrame: 
        """
        Find the segment of a pattern that can be reached withing a certain 
        time budget (i.e. 15 minutes) starting traveling from a stop.
        Inputs: 
            - df_travel_time (pl.DataFrame): A dataframe with the pid segment 
            from the bus stop to the end of the pattern that contains the 
            cumulative travel time elapsed at different bus stops. This is used
            for filtering stops within reach. 
            - df_pattern (pl.DataFrame)
            - stop (str): a stop id where the travel starts 
            - pid (str): a pattern id that goes through that stop 
            - time_budget (timedelta): Maximum amount of time that the person
            has to travel. 

        Returns: 
        - gdf_dissolved (gpd.GeoDataFrame): A data frame with a single shape 
        of the path that can be reached within the time budget. 
        """
        # logging.debug(f"{time_budget = }")
        # logging.debug(f"{df_travel_time = }")

        # Filter observations based on the windows
        df_within_time = df_travel_time.filter(pl.col("travel_time_elapsed") <= time_budget) 

        # logging.debug(f"{df_within_time = }")
        stops_within_reach = df_within_time["stop_id"].unique()
        

        # Filter original pattern stops 
        df_pattern_within_reach = df_pattern.filter(pl.col("stpid").is_in(stops_within_reach))
        df_pattern_within_reach = df_pattern_within_reach.to_pandas()
        
        # Compute geometry 
        gdf = ppatt.convert_to_geometries(df_pattern_within_reach, pid = pid, write = False)

        # TODO: Add walking distance buffer to stops

        # Merge all shapes into a single one and save 
        gdf_dissolved = gdf.dissolve()

        gdf_dissolved["pid"] = pid
        gdf_dissolved["origin_stop"] = stop 
        gdf_dissolved["time_budget"] = time_budget

        return gdf_dissolved


def build_stops_reachable_areas(df_catalogue: pl.DataFrame, df_stops_metrics: pl.DataFrame) -> gpd.GeoDataFrame: 
    """
    Finds areas that are reachable for each stop for different time frames. 
    """
    
    # Bring global variables to function scope
    pid_dir = DIR_PID
    time_windows = TIME_WINDOWS

    gdf_stops_reach_areas = gpd.GeoDataFrame()

    # CHANGED IMPLEMENTATION TO ONLY GO THROUGH STOP/PIDS PAIRS IN METRICS
    all_stops = df_stops_metrics["stop_id"].unique()
    processed_stops = 0
    # for stop in df_catalogue["stop_id"].unique(): 
    for stop in all_stops: 
  
        # Identify PIDS that go through stop using the catalogue 
        df_stop = df_stops_metrics.filter(pl.col("stop_id") == stop)
        pids_in_stop = df_stop["pid"].unique()

        logging.info(f"STOP ID: {stop}, bus patterns: {list(pids_in_stop)}")

        for pid in pids_in_stop: 
            
            # # TODO remove
            # stop = '8010'
            # pid = '8180'

            logging.info(f"\tComputing time for {stop = } and {pid = }")

            # Compute time for remaining path starting from the stop 
            df_time = compute_travel_time_baseline(df_catalogue, df_stops_metrics, pid = pid, stop = stop)

            if df_time is None: 
                print(f"Skiped pid {pid}")
                continue

            # Load pattern, no need to preserve geometry 
            df_pattern = pl.read_parquet(f"{pid_dir}/patterns_current/pid_{pid}_stop.parquet"
                                         ).with_columns(
                                             stpid = pl.col("stpid").forward_fill()
                                             ).drop(["geometry"])
    
            for time_budget in time_windows:
                # logging.debug(f"\t\t{time_budget = }")
                gdf_dissolved = find_reachable_segment_by_time(df_time, df_pattern, stop = stop, pid = pid, time_budget = time_budget)
                # logging.debug(f"{gdf_dissolved =}")
                gdf_stops_reach_areas = pd.concat([gdf_stops_reach_areas, gdf_dissolved])
                logging.info(f"\tFinished processing all times for {stop = } and {pid = }")
            
        processed_stops += 1
        logging.info(f"Processed {processed_stops} out of {len(all_stops)} stops: {(processed_stops*100)/len(all_stops)}%\n\n")

        # # TODO: Remove example cap 
        # if processed_stops == 2: 
        #     return gdf_stops_reach_areas
            
    return gdf_stops_reach_areas

 

def merge_areas_by_time(gdf_stops_reach_areas: gpd.GeoDataFrame) -> gpd.GeoDataFrame: 
    """
    Takes all the reachable areas for stop-pid-time combination and merges
    the shape by stop-time. 
    """

    # Standardize column names and clean time labels
    gdf_stop_areas = gdf_stops_reach_areas.rename(columns={"stpid": "stop_id"})

    # TODO: Apply dissolved grouped by stop_id and time budget
    gdf_dissolved = gdf_stop_areas.dissolve(by = "time_budget").reset_index()
    gdf_dissolved = gdf_stop_areas

    # logging.info(f"{gdf_dissolved = }")

    gdf_dissolved["minutes"] = gdf_dissolved["time_budget"].transform(
        lambda x: x.seconds / 60
    )

    # TODO: Pasar implementación a vectores con los valores del tiempo 
    gdf_dissolved["time_label"] = gdf_dissolved["minutes"].case_when(
        [
            (gdf_dissolved.eval(f"minutes == {str(2)}"), "2 minutes"), 
            (gdf_dissolved.eval(f"minutes == {str(5)}"), "5 minutes"), 
            (gdf_dissolved.eval(f"minutes == {str(10)}"), "10 minutes"), 
            (gdf_dissolved.eval(f"minutes == {str(15)}"), "15 minutes"),
            (gdf_dissolved.eval(f"minutes == {str(30)}"), "30 minutes"),
            (gdf_dissolved.eval(f"minutes == {str(60)}"), "1 hour"),
            (gdf_dissolved.eval(f"minutes == {str(90)}"), "1 hour 30 minutes"),
            (gdf_dissolved.eval(f"minutes == {str(120)}"), "2 hours")
        ]
    )


    # gdf_dissolved["time_label"] = pd.Categorical(gdf_dissolved["time_label"], 
    #                                               categories = ["2 minutes", 
    #                                                             "5 minutes",
    #                                                             "10 minutes", 
    #                                                             "15 minutes"])

    
    gdf_dissolved["time_label"] = pd.Categorical(gdf_dissolved["time_label"], 
                                                  categories = [ 
                                                                "5 minutes", 
                                                                "10 minutes", 
                                                                "15 minutes",
                                                                "30 minutes",
                                                                "1 hour",
                                                                "1 hour 30 minutes",
                                                                "2 hours"])

    return gdf_dissolved

if __name__ == "__main__":
   
    # Load catalogue 
    df_catalogue = pl.read_parquet("../../scrapers/rt_pid_stop.parquet").with_columns(
        # Cast types so that ids are the same in both data sets
        pl.col("pid").cast(pl.Int16).cast(pl.String),
        pl.col("stop_id").cast(pl.Int16).cast(pl.String)
    )


    # Clean prepare stop metrics for processing 
    df_stops_metrics = prepare_input_dataset()

        # Time to previous stop is 
    gdf_stops_reach_areas = build_stops_reachable_areas(df_catalogue, df_stops_metrics)
    gdf_stop_areas = merge_areas_by_time(gdf_stops_reach_areas)

    # logging.debug(f"{gdf_stop_areas = }")
    # logging.debug(f"{gdf_stop_areas.columns}")

    gdf_stop_areas.to_parquet("stop_access_shapes.parquet")

    # Print running time
    total_running_time = time.time() - start_tmstmp
    formatted_time = time.strftime(
    "%H hours %M minutes %S seconds", time.gmtime(total_running_time)
)
    logging.info(f" Total running time {formatted_time}")
    logging.info(f"{'-'*69}")
