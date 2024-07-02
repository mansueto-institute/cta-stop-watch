# For each bus stop 
    # Identify every pid that goes through it 
    # Merge into a single stop grid 
    # Store grid? 

# IMPORTS ---------------------------------------------------------------------
import polars as pl 
import pandas as pd
import geopandas as gpd
import pathlib
import os
import re
import logging
import time
from polars import ColumnNotFoundError
import datetime

# Import from module with "-" on its name 
import importlib  
ppatt = importlib.import_module("cta-stop-watch.cta-stop-etl.process_patterns")

PID_DIR = pathlib.Path(__file__).parents[2] / "cta-stop-etl/out/"
TIME_WINDOWS = [5, 10, 15, 20, 30, 45, 60, 90, 120]
TIME_WINDOWS = [datetime.timedelta(minutes=t) for t in TIME_WINDOWS]


# LOGGER ----------------------------------------------------------------------

logger = logging.getLogger(__name__)

logging.basicConfig(
    filename="stops_dict.log",
    filemode="w",
    encoding="utf-8",
    level=logging.DEBUG,
    # level=logging.INFO
)

start_tmstmp = time.time()
start_string = time.asctime(time.localtime())
logging.info(f"CTA BUSES ETL PIPELINE STARTED AT: {start_string}")


# CONTANTS --------------------------------------------------------------------

# Paths 
DIR = pathlib.Path(__file__) 
DIR_INP = DIR.parents[2] / "cta-stop-etl/out/"
print(DIR_INP)

# FUNCTIONS -------------------------------------------------------------------

# def get_stops_id(path: pathlib.Path) -> list[int]: 
#     gf_stops = gpd.read_file(path)
#     stops_id = list(gf_stops["SYSTEMSTOP"].astype(int))
#     return stops_id


def create_stop_pid_xwalk() -> dict:
    # Stops
    stops_patterns_dict = {}
    pid_stops_files = [f for f in os.listdir(DIR_INP / "patterns_current") if f.endswith("_stop.parquet")]

    # logging.debug(f"{pid_stops_files = }")
    
    # Loop over all patterns 
    for pid_file in pid_stops_files: 
        # Extract pid 
        numbers = re.findall(r"\d+", pid_file)
        pid = numbers[0]

        # Import parquet file 
        df = pl.read_parquet(DIR_INP /  "patterns_current" / pid_file)

        # Find all stops within pattern
        pid_stops = df["stop_id"].unique()

        # Add pid to stops dictionary 
        for stop in pid_stops: 
            if stop not in stops_patterns_dict: 
                stops_patterns_dict[stop] = {"patterns": [pid]}
            else: 
                stops_patterns_dict[stop]["patterns"].append(pid)

    # logging.debug(stops_patterns_dict)
    # logging.debug(f"Total stops found in patterns: {len(stops_patterns_dict)}")

    return stops_patterns_dict

def compute_travel_time(df_catalogue, stop, pid, initial_wait = 0):
    """ 
    Initial wait is any metric used to reflect the time a user waits 
    at the bus stop for the bus to arrive 
    """

    logging.debug(f"Trip ID: {pid = }")

    df_stop_pid = df_catalogue.filter(
        (pl.col("stop_id") == stop) & (pl.col("pid") == pid)
    )

    logging.debug(df_stop_pid["route_id"])
    route = df_stop_pid["route_id"].item()
    logging.debug(f"{route = }")


    # TODO update real dir where time data is stored 
    # df_time_full_pid = pl.read_parquet(DIR_INP /  f"time_to_previous_stop_{stop}.parquet")
    df_time_full_pid = pl.read_parquet(f"time_to_previous_stop_{route}.parquet")

    # logging.debug(f"{df_time_full_pid}")
    logging.debug(f"Column names: {df_time_full_pid.columns}")

    # logging.debug(f"{df_time_full_pid}")
    # logging.debug(f"{int(pid) in df_time_full_pid['pid'].unique()}")

    df_pid = df_time_full_pid.filter((pl.col("pid") == int(pid) ) & (pl.col("period") == "year")
                                    ).sort(pl.col("stop_sequence")
                                    ).rename({"median_actual_time_to_previous_stop": "time_to_previous"}
                                    ).select(["rt", "pid", "stop_sequence", "stop_id", "time_to_previous"]
        )
    
    logging.debug(f"{df_pid}")


    start = df_pid.with_row_index().filter(pl.col("stop_id") == stop)["index"].item()
    end = len(df_pid)
    df_remaining_path = df_pid.slice(start, end)
    
    start_time = df_pid.filter(pl.col("stop_id") == stop)["time_to_previous"].item()
    df_time = df_remaining_path.with_columns(
        travel_time_elapsed = pl.cum_sum("time_to_previous") - start_time + initial_wait, 
    )

    # Find index of stop and start from there 
    return df_time

# def build_geometry_for_pid_section(): 
#     ppatt.convert_to_geometries()
#     pass


def time_budget(df_catalogue): 
    
    pid_dir = PID_DIR
    time_windows = TIME_WINDOWS

    gdf_stops_reach_area = gpd.GeoDataFrame()

    for stop in df_catalogue["stop_id"].unique(): 
        df_stop = df_catalogue.filter(pl.col("stop_id") == stop)
                    
        logging.debug(f"Bust stop ID: {stop}")
        stop_pids = df_stop["pid"].to_list()
        logging.debug(f"Bus stop patterns: {stop_pids}")

        for pid in stop_pids: 
            # TODO remove hard coded PID
            stop = "14886"
            logging.info(f"{pid = }")

            # Compute time for remaining path starting from the stop 
            # Convert segment into geometry
            df_time = compute_travel_time(df_catalogue, pid = pid, stop = stop)

            # Load patterns with gdp to preserve geometry 
            # gdf_raw_pattern = gpd.read_parquet(f"{pid_dir}/patterns_current/pid_{int(pid)}_stop.parquet") 
            
            # logging.debug(f"{gdf_raw_pattern = }")

            df_pattern = pl.read_parquet(f"{pid_dir}/patterns_current/pid_{int(pid)}_stop.parquet"
                                         ).with_columns(
                                             stpid = pl.col("stpid").forward_fill()
                                             ).drop(["geometry"])
            
            # logging.debug(f"{df_pattern['stop_id']}")

            logging.debug(f"{df_pattern.columns = }")

            # df_raw_pattern = gpd.read_parquet(f"{pid_dir}/patterns_current/pid_{int(pid)}_stop.parquet"
            #                                  ).with_columns(stpid = pl.col("stpid").forward_fill()
            #                                  ).rename({"stpid": "stop_id"})            

            # df_raw_pattern = pl.read_parquet(f"{pid_dir}/pids/{int(pid)}.parquet")
            # logging.info(f"{df_pattern = }")

            for time_budget in time_windows:
                # Filter observations based on the windows
                df_within_time = df_time.filter(pl.col("travel_time_elapsed") < time_budget) 
                stops_within_reach = df_within_time["stop_id"].unique()

                # Filter original pattern stops 
                df_pattern_within_reach = df_pattern.filter(pl.col("stpid").is_in(stops_within_reach))

                # logging.debug(f"{df_pattern_within_reach}")

                # logging.debug(list(df_pattern_within_reach["geometry"]))
                
                df_pattern_within_reach = df_pattern_within_reach.to_pandas()
                
                # logging.debug(f"{type(df_pattern_within_reach) = }")
                # logging.debug(df_pattern_within_reach)
                # Compute geometry 
                gdf = ppatt.convert_to_geometries(df_pattern_within_reach, pid = pid, write = False)

                # TODO: Add walking distance buffer depending on remaining time

                # Merge all shapes into a single one and save 
                gdf_dissolved = gdf.dissolve()

                gdf_dissolved["pid"] = pid
                gdf_dissolved["origin_stop"] = stop 
                gdf_dissolved["time_budget"] = time_budget
                gdf_stops_reach_area = pd.concat([gdf_stops_reach_area, gdf_dissolved])
        
                logging.debug(f"{gdf_stops_reach_area = }")

        break

        logging.debug(f"{gdf_stops_reach_area = }")


def main(): 
    # Load stop id and patterns catalogue 
    df_catalogue = pl.read_parquet("../../scrapers/rt_pid_stop.parquet")

    # logging.debug(df_catalogue)
    # logging.debug(df_catalogue.columns)
    
    # Time to previous stop is 
    time_budget(df_catalogue)


if __name__ == "__main__":
    main()
