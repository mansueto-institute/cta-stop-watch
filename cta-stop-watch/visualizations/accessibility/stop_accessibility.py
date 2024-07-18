# TODO
# Debug for Hyde Park
# Clean and document
# For discrete implementation
# Make sure times over highest window is not getting filtered out
# For discrete implementation
# Adapt a function to unite shapes instead of having single dots

# IMPORTS ---------------------------------------------------------------------

import os
import polars as pl
from polars.exceptions import PanicException
import pandas as pd
import geopandas as gpd
from pyproj import CRS
import pathlib
import logging
import time
import datetime
from accessibility_maps import find_community_stops, get_all_communities
import importlib  # Import from module with "-" on its name

ppatt = importlib.import_module("cta-stop-watch.cta-stop-etl.process_patterns")


# CONTANTS --------------------------------------------------------------------

# Simulation parameters
DISCRETE_TIME_BINS = True
COMMUNITY = True
NUM_TRANSFERS = 0
YEAR = 2024
COMMUNITY = True

# Paths
DIR = pathlib.Path(__file__)
DIR_INP = DIR.parents[2] / "cta-stop-etl/out/"
DIR_PID = pathlib.Path(__file__).parents[2] / "cta-stop-etl/out/"
DIR_SHAPES = DIR.parents[2] / "shapefiles/"


# Simulation parameter: Time in minutes
TIME_WINDOWS = [5, 10, 15, 30, 60, 90, 120]
# TIME_WINDOWS = [2, 5, 10, 15]
TIME_WINDOWS = [datetime.timedelta(minutes=t) for t in TIME_WINDOWS]

# CRS
PROJ_4326 = CRS("epsg:4326")
PROJ_26971 = CRS("epsg:26971")


# Data
DF_CATALOGUE = pl.read_parquet("../../scrapers/rt_pid_stop.parquet").with_columns(
    # Cast types so that ids are the same in both data sets
    pl.col("pid").cast(pl.Int16).cast(pl.String),
    pl.col("stop_id").cast(pl.Int16).cast(pl.String),
)

# LOGGER ----------------------------------------------------------------------

# Start logger for this script
logger = logging.getLogger(__name__)

logging.basicConfig(
    filename="prepare_data.log",
    filemode="w",
    # format = "%(levelname)-8s [%(filename)s:%(lineno)d] %(message)s",
    format="%(asctime)s %(levelname)-3s %(message)s",
    encoding="utf-8",
    level=logging.DEBUG,
    # level=logging.INFO,
    datefmt="%H:%M",
    force=True,
)
start_tmstmp = time.time()
start_string = time.asctime(time.localtime())
logging.info(f"SCRIPT STARTED AT {start_string}")
logging.info(f"{'-'*68}\n")


# FUNCTIONS -------------------------------------------------------------------


def prepare_input_dataset():

    logging.info("Merging bus performance metrics with bus stop sequence")

    # Dictionary for bus stops id, bus stop sequence, and pattern id
    df_stop_seq = (
        pl.read_parquet("df_stop_pid_seq.parquet")
        .with_columns(
            # Cast types so that ids are the same in both data sets
            pl.col("pid").cast(pl.Int16).cast(pl.String),
            pl.col("stop_id").cast(pl.Int16).cast(pl.String),
        )
        .unique()
    )

    # Stop sequence info: filter for relevant time period
    df_stops_metrics = pl.read_parquet("actual_stop_metrics_df.parquet")

    df_stops_metrics = df_stops_metrics.filter(
        pl.col("period_value") == YEAR
    ).with_columns(
        # Cast types so that ids are the same in both data sets
        pl.col("pid").cast(pl.Int16).cast(pl.String),
        pl.col("stop_id").cast(pl.Int16).cast(pl.String),
    )

    # Join actual metrics with sequence order of bus stops
    df_stops_metrics = df_stops_metrics.join(
        df_stop_seq, how="left", on=["stop_id", "pid"], coalesce=True
    )

    return df_stops_metrics


def compute_travel_time_baseline(
    df_stops_metrics: pl.DataFrame, stop: str, pid: str, df_catalogue: pl.DataFrame
) -> pl.DataType:
    """
    Takes a pair of a stop and a pattern and compute the travel time it would
    take to go from that stop to the end of that pattern. Notice that the stop
    is not necessarily the starting point of the pattern.

    Inputs:
        - df_stops_metrics (pl.DataFrame): Bus performance metrics computed
        from actual bus data
        - stop (str): a stop id where the travel starts
        - pid (str): a pattern id that goes through that stop
        - initial_wait (timedelta): Any metric used to reflect the time a user
        waits at the bus stop for the bus to arrive.
        - df_catalogue (pl.DataFrame): A dataframe with valid pairs of stops
        and pids, with information of the route the pair corresponds to.
    Returns:
        - df_travel_time (pl.DataFrame): A dataframe with the pid segment from
        the bus stop to the end of the pattern that contains the cumulative
        travel time elapsed at different bus stops.
    """

    # Filter bus stop' pattern info for corresponding time period a
    df_pid = (
        df_stops_metrics.filter(
            (pl.col("pid") == pid)
            & (pl.col("period") == "year")
            & (pl.col("period_value") == YEAR)
            & True
        )
        .with_columns(pl.col("stop_sequence").cast(pl.Int16))
        .sort(pl.col("stop_sequence"))
        .rename({"median_actual_time_to_previous_stop": "time_to_previous"})
        .select(
            [
                "rt",
                "pid",
                "stop_sequence",
                "stop_id",
                "period_value",
                "time_to_previous",
            ]
        )
    )

    df_start_stop = df_pid.with_row_index().filter(pl.col("stop_id") == stop)

    # Some STOP/PID patterns will not be available for the time period
    if df_pid.shape[0] == 0:
        logging.error(f"PID {pid} not present in stop metrics")
        return None
    elif df_start_stop.shape[0] == 0:
        logging.debug(f"Bus stop {stop} not present in stop metrics for 2024")
        return None
    elif df_start_stop.shape[0] > 1:
        logging.info("More than one observation found for the stop")
        logging.debug(f"{df_start_stop = }")

        # Remove duplicates where time is the same
        df_start_stop = df_start_stop.unique(subset=["time_to_previous"])
        logging.info("After filtering for duplicate time values:")
        logging.debug(f"{df_start_stop = }")

    # Get route when bus stop is present in the catalogue
    try:
        df_stop_pid_pair = df_catalogue.filter(
            (pl.col("stop_id") == stop) & (pl.col("pid") == pid)
        )
        route = df_stop_pid_pair["route_id"].item()
    except (KeyError, ValueError) as e:
        logging.debug("Stop/pid pair not in catalogue")
        logging.error(e)
        route = None

    # Get time of the bus moving to next stops
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

    df_stop_travel_times = (
        df_remaining_path.with_columns(
            travel_time_elapsed=pl.cum_sum("time_to_previous")
            - start_time
            + initial_wait,
            route=pl.lit(route),
        )
        .with_columns(minutes=pl.col("travel_time_elapsed").dt.total_seconds() / 60)
        .with_columns(pl.col("minutes").cast(pl.Int32))
    )

    return df_stop_travel_times


def find_reachable_segment_by_time(
    df_travel_time: pl.DataFrame,
    df_pattern: pl.DataFrame,
    stop: str,
    pid: str,
    time_budget: datetime.timedelta,
) -> gpd.GeoDataFrame:
    """
    For implementation of discrete windows of time.
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

    # Filter observations based on the windows
    df_within_time = df_travel_time.filter(pl.col("travel_time_elapsed") <= time_budget)

    # logging.debug(f"{df_within_time = }")
    stops_within_reach = df_within_time["stop_id"].unique()

    # Filter original pattern stops
    df_pattern_within_reach = df_pattern.filter(
        pl.col("stpid").is_in(stops_within_reach)
    )
    df_pattern_within_reach = df_pattern_within_reach.to_pandas()

    # Compute geometry
    gdf = ppatt.convert_to_geometries(df_pattern_within_reach, pid=pid, write=False)

    # TODO: Add walking distance buffer to stops

    # Merge all shapes into a single one and save
    gdf_dissolved = gdf.dissolve()

    gdf_dissolved["pid"] = pid
    gdf_dissolved["origin_stop"] = stop
    gdf_dissolved["time_budget"] = time_budget
    gdf_dissolved["minutes"] = int(time_budget.total_seconds() / 60)

    return gdf_dissolved


def label_discrete_times(gdf):
    """
    Takes a gdf with a column of cumulative times in minutes
    """
    gdf
    # TODO: Pasar implementación a vectores con los valores del tiempo
    gdf["time_label"] = gdf["minutes"].case_when(
        [
            (gdf.eval("minutes == 5"), "5 minutes"),
            (gdf.eval("minutes == 15"), "15 minutes"),
            (gdf.eval("minutes == 30"), "30 minutes"),
            (gdf.eval("minutes == 60"), "1 hour"),
            (gdf.eval("minutes == 90"), "1 hour 30 minutes"),
            (gdf.eval("minutes == 120"), "2 hours"),
        ]
    )

    gdf["time_label"] = pd.Categorical(
        gdf["time_label"],
        categories=[
            "5 minutes",
            "10 minutes",
            "15 minutes",
            "30 minutes",
            "1 hour",
            "1 hour 30 minutes",
            "2 hours",
        ],
    )

    return gdf


def pid_travel_time_discrete(pid: str, df_stop_travel_times, stop_id):
    # Bring global variables to function scope
    pid_dir = DIR_PID
    time_windows = TIME_WINDOWS

    # Load pattern, use polars since there's no need to preserve geometry
    # Maybe change implementation to include bus stops figure
    df_pattern = (
        pl.read_parquet(f"{pid_dir}/patterns_current/pid_{pid}_stop.parquet")
        .with_columns(stpid=pl.col("stpid").forward_fill())
        .drop(["geometry"])
    )

    gdf_stop_time_shapes_pid = gpd.GeoDataFrame()

    for time_budget in time_windows:
        gdf_dissolved = find_reachable_segment_by_time(
            df_stop_travel_times,
            df_pattern,
            stop=stop_id,
            pid=pid,
            time_budget=time_budget,
        )
        gdf_stop_time_shapes_pid = pd.concat([gdf_stop_time_shapes_pid, gdf_dissolved])

    logging.info(
        f"\t\tProcessed all times in discrete bining for {stop_id = } and {pid = }"
    )

    gdf_stop_time_shapes_pid_labeled = label_discrete_times(gdf_stop_time_shapes_pid)

    return gdf_stop_time_shapes_pid_labeled


def pid_travel_time_continuous(pid: str, df_stop_travel_times):
    # Bring global variables to function scope
    pid_dir = DIR_PID

    # For continuous analysis just get shapes between points associated to time
    # gdf_pattern = gpd.read_parquet(f"{pid_dir}/patterns_current/pid_{pid}_stop.parquet").sort_values(by = "seq")
    gdf_pattern = gpd.read_parquet(f"{pid_dir}/patterns_current/pid_{pid}_stop.parquet")
    gdf_segment = gpd.read_parquet(
        f"{pid_dir}/patterns_current/pid_{pid}_segment.parquet"
    )

    # logging.debug(f"Original segment geometry {gdf_segment['geometry'] = }")

    gdf_pattern = gdf_pattern.ffill(axis="columns").rename(
        columns={"seq": "stop_sequence", "stpid": "stop_id"}
    )

    # Add pattern's segments between bus stops shape
    gdf_pattern = gdf_pattern.drop(columns=["geometry"])
    gdf_pattern["geometry"] = gdf_segment["geometry"]

    # logging.debug(f"New pattern geometry {gdf_pattern['geometry'] = }")
    # Join segment shapes with time metrics

    gdf_stop_time_shapes_pid = df_stop_travel_times.to_pandas().merge(
        gdf_pattern, on=["stop_sequence", "stop_id"]
    )

    # logging.debug(f"Geometry after merge {gdf_stop_time_shapes_pid['geometry'] = }")
    logging.info(f"\t\tProcessed continuous times for {stop_id = } and {pid = }")

    return gdf_stop_time_shapes_pid


def get_time_shapes_for_stop(
    df_stops_metrics: pl.DataFrame, stop_id: str, discrete=False
):
    """ """

    # Bring global variables to function scope
    df_catalogue = DF_CATALOGUE

    # Identify PIDS that go through stop using the catalogue
    pids_in_stop = list(
        df_stops_metrics.filter(pl.col("stop_id") == stop_id)["pid"].unique()
    )
    logging.info(f"STOP ID: {stop_id}, bus patterns: {list(pids_in_stop)}")

    gdf_stop_time_shapes = gpd.GeoDataFrame()

    if not pids_in_stop:
        return None

    for pid in pids_in_stop:
        # For a pair of stop and pattern, find time to reach to following stops
        df_stop_travel_times = compute_travel_time_baseline(
            df_stops_metrics, stop=stop_id, pid=pid, df_catalogue=df_catalogue
        )
        # logging.debug(f"{df_stop_travel_times.columns = }")

        # Proces for fixed discrete time windows
        try:
            if discrete:
                gdf_stop_time_shapes_pid = pid_travel_time_discrete(
                    pid, df_stop_travel_times, stop_id
                )
            else:
                gdf_stop_time_shapes_pid = pid_travel_time_continuous(
                    pid, df_stop_travel_times
                )
        except PanicException as e:
            logging.info(f"Skipping pid {pid} for stop {stop_id}")
            logging.error(e, exc_info=True)
            continue

        df_stop_time_shapes = pd.concat(
            [gdf_stop_time_shapes, gdf_stop_time_shapes_pid]
        )
        gdf_stop_time_shapes = gpd.GeoDataFrame(
            df_stop_time_shapes, geometry=df_stop_time_shapes["geometry"], crs=PROJ_4326
        )

        # logging.debug(f"Geometry after concat {gdf_stop_time_shapes_pid['geometry'] = }")

    # Store bus stop accessibility shapes and time
    if discrete:
        logging.info(f"Writing discrete data set for {stop_id = }\n")
        gdf_stop_time_shapes.to_parquet(
            f"out/stops_parquets/{stop_id}_discrete.parquet"
        )
    else:
        logging.info(f"Writing continuous data set for {stop_id = }\n")
        gdf_stop_time_shapes.to_parquet(
            f"out/stops_parquets/{stop_id}_continuous.parquet",
            geometry_encoding="geoarrow",
        )

    return gdf_stop_time_shapes


def get_all_shapes_community(community_name):

    community_stops = find_community_stops(community_name)

    df_stops_reach_areas = gpd.GeoDataFrame()

    for stop_id in community_stops:
        gdf_stop_time = get_time_shapes_for_stop(
            df_stops_metrics=df_stops_metrics,
            stop_id=stop_id,
            discrete=DISCRETE_TIME_BINS,
        )

        if gdf_stop_time is not None:
            df_stops_reach_areas = pd.concat([df_stops_reach_areas, gdf_stop_time])

    gdf_stops_reach_areas = gpd.GeoDataFrame(
        df_stops_reach_areas, geometry=df_stops_reach_areas["geometry"], crs=PROJ_4326
    )

    type_timing = "discrete" if DISCRETE_TIME_BINS else "continuous"

    logging.info(f"Writing {type_timing} data set for {community_name} stops")
    gdf_stops_reach_areas.to_parquet(
        f"out/communities/{community_name}_stops_{type_timing}.parquet"
    )

    return True


def print_script_running_time(start_tmstmp):
    # Print running time
    logging.info(f"{'-'*68}")
    total_running_time = time.time() - start_tmstmp
    formatted_time = time.strftime(
        "%H hours %M minutes %S seconds", time.gmtime(total_running_time)
    )
    logging.info(f"Total running time {formatted_time}")
    logging.info(f"{'-'*68}")


# IMPLEMENTATION --------------------------------------------------------------

if __name__ == "__main__":

    # Clean prepare stop metrics for processing
    df_stops_metrics = prepare_input_dataset()

    # Utils for looping over stops and storing information
    all_stops = df_stops_metrics["stop_id"].unique()
    processed_stops = 0

    # Stop in a single community
    if COMMUNITY:
        logging.info("Process for communities\n")
        all_communities = get_all_communities()
        count = 0
        for community in all_communities:
            count += 1

            # Avoid repeating process for already computed communities
            type_timing = "discrete" if DISCRETE_TIME_BINS else "continuous"
            if os.path.exists(
                f"out/communities/{community}_stops_{type_timing}.parquet"
            ):
                logging.info(f"{'-'*68}")
                logging.debug(
                    f"{community} ALREDAY COMPUTED - TOTAL: {count} / {len(all_communities)} \n"
                )
                continue

            logging.info(f"{'-'*68}")
            logging.debug(
                f"ANALYSIS FOR {community}: {count} / {len(all_communities)} \n"
            )

            try:
                get_all_shapes_community(community)
            except Exception as e:
                logging.error(f"Failed to compute stops for {community}\n")
                logging.error(e, exc_info=True)
    else:
        logging.info("Process for all stops\n")

        df_stops_reach_areas = gpd.GeoDataFrame()

        for stop_id in all_stops:
            gdf_stop_time = get_time_shapes_for_stop(
                df_stops_metrics=df_stops_metrics,
                stop_id=stop_id,
                discrete=DISCRETE_TIME_BINS,
            )
            if gdf_stop_time is not None:
                df_stops_reach_areas = pd.concat([df_stops_reach_areas, gdf_stop_time])

        gdf_stops_reach_areas = gpd.GeoDataFrame(
            df_stops_reach_areas,
            geometry=df_stops_reach_areas["geometry"],
            crs=PROJ_4326,
        )

    # gdf_stops_reach_areas.to_parquet("stop_access_shapes.parquet")
    print_script_running_time(start_tmstmp)
