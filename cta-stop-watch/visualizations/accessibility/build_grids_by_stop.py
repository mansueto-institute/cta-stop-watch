
# Script logic  
# For bus stops 
    # For every pid that goes throug it
        # For different time budgets
            # Convert segment into shape 

# TODO
# Check projection 
# Address edge cases (stop is start/end of path)
# Dissolve shapes grouping by bus stop and time budget
# Add transfers to simulation 
# Add initial wait based on actual waiting and delay times
# Add walking time from final bus stop within time budget
# Format final time as inteer instead of timedelta/string 
    # (df['tdColumn'] = pd.to_numeric(df['tdColumn'].dt.days, downcast='integer')) 
    # Source: https://stackoverflow.com/questions/25646200/python-convert-timedelta-to-int-in-a-dataframe


# Remove hard coded stop_id and pid example for the 172 

# IMPORTS ---------------------------------------------------------------------

import polars as pl 
import pandas as pd
import geopandas as gpd
import pathlib
import os
import logging
import time
import datetime
from selenium import webdriver
from selenium.webdriver.remote.remote_connection import LOGGER


# Import from module with "-" on its name 
import importlib  
ppatt = importlib.import_module("cta-stop-watch.cta-stop-etl.process_patterns")


# LOGGER ----------------------------------------------------------------------

# Set selenium logger to warning level
LOGGER.setLevel(logging.WARNING)


# Start logger for this script
logger = logging.getLogger(__name__)

logging.basicConfig(
    filename="stops_dict.log",
    filemode="w",
    format = "%(levelname)-8s [%(filename)s:%(lineno)d] %(message)s",
    encoding="utf-8",
    level=logging.DEBUG,
    # level=logging.INFO
)

# formatter = logging.Formatter('[%(asctime)s] p%(process)s {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s','%m-%d %H:%M:%S')

start_tmstmp = time.time()
start_string = time.asctime(time.localtime())
logging.info(f"CTA BUSES ETL PIPELINE STARTED AT: {start_string}")


# CONTANTS --------------------------------------------------------------------

# Paths 
DIR = pathlib.Path(__file__) 
DIR_INP = DIR.parents[2] / "cta-stop-etl/out/"
DIR_PID = pathlib.Path(__file__).parents[2] / "cta-stop-etl/out/"


# Simulation parameter: Time
# TIME_WINDOWS = [5, 10, 15, 20, 30, 45, 60, 90, 120]
TIME_WINDOWS = [2, 5, 10, 15]
TIME_WINDOWS = [datetime.timedelta(minutes=t) for t in TIME_WINDOWS]

# Simulation parameter: Tranfers
NUM_TRANSFERS = 0

# Data 
# DF_CATALOGUE = pl.read_parquet("../../scrapers/rt_pid_stop.parquet").with_columns(
#         # Cast types so that ids are the same in both data sets
#         pl.col("pid").cast(pl.Int16).cast(pl.String), 
#         pl.col("stop_id").cast(pl.Int16).cast(pl.String)
#     )


# FUNCTIONS -------------------------------------------------------------------

def compute_travel_time_baseline(df_catalogue: pl.DataFrame, df_stops_metrics: pl.DataFrame, stop: str, pid:str) -> pl.DataType:
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

    # Get route 
    df_stop_pid_pair = df_catalogue.filter(
        (pl.col("stop_id") == stop) & (pl.col("pid") == pid))
    route = df_stop_pid_pair["route_id"].item()

    # Valid pair 
    # logging.info(f"{df_stop_pid_pair = }")


    # logging.debug(df_stops_metrics.columns)
    # logging.debug(f"Pid before filtering {pid = }")

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

    # logging.debug(df_pid.with_row_index().filter(pl.col("stop_id") == stop))
    
    # Find index of stop and take it as the start of the path  
    start = df_pid.with_row_index().filter(pl.col("stop_id") == stop)["index"].item()
    end = len(df_pid)
    df_remaining_path = df_pid.slice(start, end)
    
    # Edge cases (bus stop is the start or the end of the pid)
    if len(df_remaining_path) == 1: 
        pass

    # Compute total travel time with bus stop as start of the travel 
    # Note: Be careful handling time operations

    start_time = df_pid.filter(pl.col("stop_id") == stop)["time_to_previous"].item()
    initial_wait = datetime.timedelta(0)

    # logging.debug(f"{type(start_time) = }")
    # logging.debug(f"{type(initial_wait) = }")

    df_travel_time = df_remaining_path.with_columns(
        travel_time_elapsed = pl.cum_sum("time_to_previous") - start_time + initial_wait, 
        # travel_time_elapsed = pl.cum_sum("time_to_previous"), 
        route = pl.lit(route)
    )

    # logging.debug(df_travel_time)

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

        # TODO: Add walking distance buffer depending on remaining time

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

    for stop in df_catalogue["stop_id"].unique(): 
        # TODO remove hard coded STOP ID
        # stop = "1525"

        df_stop = df_catalogue.filter(pl.col("stop_id") == stop)
                    
        logging.debug(f"Bust stop ID: {stop}")
        stop_pids = df_stop["pid"].to_list()
        logging.debug(f"Bus stop patterns: {stop_pids}")

        for pid in stop_pids: 
            # Compute time for remaining path starting from the stop 
            df_time = compute_travel_time_baseline(df_catalogue, df_stops_metrics, pid = pid, stop = stop)

            # Change number format (remove leading zeros)
            logging.info(f"{pid = }")

            # Load pattern, no need to preserve geometry 
            df_pattern = pl.read_parquet(f"{pid_dir}/patterns_current/pid_{pid}_stop.parquet"
                                         ).with_columns(
                                             stpid = pl.col("stpid").forward_fill()
                                             ).drop(["geometry"])
    
            for time_budget in time_windows:
                # logging.debug(f"{time_budget = }")
                gdf_dissolved = find_reachable_segment_by_time(df_time, df_pattern, stop = stop, pid = pid, time_budget = time_budget)
                # logging.debug(f"{gdf_dissolved =}")
                gdf_stops_reach_areas = pd.concat([gdf_stops_reach_areas, gdf_dissolved])

    return gdf_stops_reach_areas

def merge_areas_by_time(gdf_stops_reach_areas: gpd.GeoDataFrame) -> gpd.GeoDataFrame: 
    """
    Takes all the reachable areas for stop-pid-time combination and merges
    the shape by stop-time. 
    """
    # TODO: Apply dissolved grouped by stop_id and time budget
    
    # Standardize column names
    gdf_stop_areas = gdf_stops_reach_areas.rename(columns={"stpid": "stop_id"})
    gdf_stop_areas["time_budget"] = gdf_stop_areas["time_budget"].astype(str)

    return gdf_stop_areas

def plot_paths(gdf: gpd.GeoDataFrame): 
    
    for stop_id in list(gdf["origin_stop"].unique()): 
        
        gdf_stop = gdf[gdf["origin_stop"] == stop_id]

        gdf_stop.sort_values("time_budget")

        # Reorder layers (shortest travel times on top)
        gdf = gdf.iloc[::-1]


        # Generate map 
        map_viz = gdf.explore(column = "time_budget", cmap = "OrRd")

        # 
        map_file = "map.html"
        map_viz.save(map_file)
        
        map_url = 'file://{0}/{1}'.format(os.getcwd(), map_file)
        
        driver = webdriver.Firefox()
        driver.get(map_url)
        time.sleep(5)
        driver.save_screenshot(f"maps/map_stop_{stop_id}.png")
        driver.quit()


    # img_data = m._to_png(5)
    # img = Image.open(io.BytesIO(img_data))
    # img.save("example.png")


def main(): 
    # Load stop id and patterns catalogue 
    df_catalogue = pl.read_parquet("../../scrapers/rt_pid_stop.parquet").with_columns(
        # Cast types so that ids are the same in both data sets
        pl.col("pid").cast(pl.Int16).cast(pl.String), 
        pl.col("stop_id").cast(pl.Int16).cast(pl.String)
    )

    # Remove this filter once we have data for every route and not just the 172
    df_catalogue = df_catalogue.filter(pl.col("route_id") == "172")

    # Load metrics 
    df_stops_metrics = pl.read_parquet("stop_metrics_df.parquet").with_columns(
        # Cast types so that ids are the same in both data sets
        pl.col("pid").cast(pl.Int16).cast(pl.String), 
        pl.col("stop_id").cast(pl.Int16).cast(pl.String)
    )

    # logging.debug(df_catalogue)
    # logging.debug(df_catalogue.columns)
    
    # Time to previous stop is 
    gdf_stops_reach_areas = build_stops_reachable_areas(df_catalogue, df_stops_metrics)
    gdf_stop_areas = merge_areas_by_time(gdf_stops_reach_areas)

    logging.debug(f"{gdf_stop_areas = }")
    logging.debug(f"{gdf_stop_areas.columns}")

    # gdf_stop_areas.to_parquet("accessibility_trial.parquet")

    # Plot and store one stop 
    plot_paths(gdf_stop_areas)


if __name__ == "__main__":
    main()
