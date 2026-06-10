# Libraries ------------------------------------------------------------------- 

import polars as pl 
import pathlib 
from datetime import datetime
from utils import debug_logger

# Constants --------------------------------------------------------------------

DATA_PATH = pathlib.Path("data/processed_by_pid")

# Load data --------------------------------------------------------------------

def get_all_pids(): 
    df_pids = pl.read_parquet("data/rt_to_pid.parquet")
    pids = df_pids.select(["pid"]).unique().to_series().to_list()
    return pids 

def pid_is_updated(filename: str): 
    print(filename)
    df_pid = pl.read_parquet(filename) 
    bus_time = df_pid["bus_stop_time"]

    #print(df_pid.columns)
    #print(type(df_pid["bus_stop_time"]))
    
    min_date = bus_time.min()
    max_date = bus_time.max()
   
    if max_date > datetime(2025, 9, 1): 
        return True 
    return False

# Implementation ---------------------------------------------------------------

if __name__ == "__main__": 
    df_pids = pl.read_parquet("data/rt_to_pid.parquet")
    routes = df_pids.select(["rt"]).unique().to_series().to_list()
    
    faulty_pids = []
    missing_pids = []
    outdated_pids = []
    
    outdated_routes = []

    for rt in routes: 
        
        pids = df_pids.filter(pl.col("rt") == rt).select(["pid"]).to_series().to_list()
        route_num_pids = len(pids) 
        
        route_missing = []
        route_faulty = []
        route_outdated = []

        for pid in pids: 

            filename = DATA_PATH / f"trips_{pid}_full.parquet"

            try: 
                updated = pid_is_updated(filename)
            except FileNotFoundError: 
                missing_pids.append(pid)
                route_missing.append(pid)
            except Exception as e: 
                print(f"An unexpected error ocurred while reading {pid}. See full error: {e}")
                faulty_pids.append(pid)  
                route_fualty.append(pid)
            if not updated: 
                outdated_pids.append(pid) 
                route_outdated.append(pid) 

        total_errors = len(route_missing) + len(route_faulty) + len(route_outdated) 
        
        if len(route_outdated) > 0: 
            outdated_routes.append(rt) 
            print(f"Route {rt} has outdated pids: \n {route_outdated}") 

            if len(route_outdated) == route_num_pids:
                debug_logger.info(f"ALL PIDS for {rt=} are outdated")

    print(f"\nFinished checking pids. The following pids are not updated: \n {outdated_pids}")
    debug_logger.info(f"\nFinished checking pids. The following pids are not updated: \n {outdated_pids}")
    print(f"\nOutdated routes: {outdated_routes}")

# EOF. ------------------------------------------------------------------------- 
