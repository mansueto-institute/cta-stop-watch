import os
import pandas as pd
import geopandas as gpd
from shapely import LineString
import pathlib
import re
import logging
import time
import pyarrow


# Logging set up --------------------------------------------------------------

# We no longer use this logging configuration.
# Logger now starts from etl_pipeline.py.

# logger = logging.getLogger(__name__)
# logging.basicConfig(
#     filename="process_patterns.log", filemode="w", encoding="utf-8", level=logging.INFO
# )
# logging.info(f"Log initialized at {time.time()}")

# Constants -------------------------------------------------------------------

## CONSTANTS
M_TO_FT = 3.280839895
BUFFER_DIST = 50

DIR_INP = pathlib.Path(__file__).parent.parent / "scrapers/out/gtfs/"
DIR_OUT = pathlib.Path(__file__).parent / "out"

# Functions -------------------------------------------------------------------


def convert_to_geometries(df_raw: pd.DataFrame, type:str) -> tuple[pd.DataFrame]:
    """
     and converts
    it into route polygones that can be used to asses if a bus is inside it's
    route and identify which are the clostests bus stops.

    The output is written locally as parquet files for every pid, the files
    are then manually moved to a box folder.

    Input:
        - pattern (df): The pattern id to call from the CTA API

    Returns:
        - A boolean  indicating if a data frame for the segment was created

    Outputs:
    - Parquet file with segments as points: out/pattern/pid_{pid}_stop.parquet
    - Parquet file with segments as buffers: "out/pattern/pid_{pid}_segment.parquet"

    """

    # Convert into geodata with projection for Chicago (EPSG 4326)
    df_pattern = gpd.GeoDataFrame(
        df_raw,
        geometry=gpd.GeoSeries.from_xy(
            x=df_raw.loc[:, "lon"], y=df_raw.loc[:, "lat"], crs="EPSG:4326"
        ),
    )

    if type == 'stop':
        # create unique id for each stop on the pattern
        df_pattern["pid_str"] = df_pattern['pid'].astype(float, errors='ignore').astype(int).astype(str)
        df_pattern["p_stp_id"] =  df_pattern["pid_str"] + "-" + df_pattern["stop_id"]
        

    else:

        df_pattern = df_pattern.sort_values(by="seq")

        # Each pair of points consitutes a segment, assign id for future grouping
        df_pattern.loc[:, "segment"] = range(0, len(df_pattern))

        # Build line geometries with each segment (pair of points)
        segments = list(range(0, len(df_pattern)))
        geometries = []

        # Because we're dealing with an sliced data frame, indices must be restarted
        # to be able to iterate over them without running into issues of being out
        # of range
        df_pattern = df_pattern.reset_index(drop=True)

        for idx, segment_data in df_pattern.iterrows():
            # The first bus stop is stored as a point geometry instead of line
            if idx == 0.0:
                geometries.append(segment_data["geometry"])
                continue
            previous_point = df_pattern.iloc[idx - 1]["geometry"]
            geometry = LineString([previous_point, segment_data["geometry"]])
            geometries.append(geometry)

        # Change projection and units so distance and time can be computed (in feet)
        df_segment = gpd.GeoDataFrame(
            data={"segments": segments}, geometry=geometries, crs="EPSG:4326"
        ).sort_values("segments")
        df_segment.loc[:, "length_ft"] = (
            df_segment.geometry.to_crs("EPSG:26971").length * M_TO_FT
        )
        df_segment.loc[:, "ls_geometry"] = df_segment.geometry
        df_segment.geometry = (
            df_segment.geometry.to_crs("EPSG:26971").buffer(BUFFER_DIST).to_crs("EPSG:4326")
        )



    return df_pattern, df_segment


def write_patterns(
    pid: str, df: pd.DataFrame, type: str, path: pathlib.Path
):

    # Parse id as integers to remove leading zeros
    try:
        pid = int(pid)
    except ValueError:
        pass

    if type == 'stop':
        print(f"Writing {path}/pid_{pid}_stop.parquet")
        df.to_parquet(f"{path}/pid_{pid}_stop.parquet")
    else:
        print(f"Writing {path}/pid_{pid}_segment.parquet")
        df.to_parquet(f"{path}/pid_{pid}_segment.parquet")

   

    return True


# Implementation (main) -------------------------------------------------------


def main():

    logging.info("Running pattern processor from archival GTFS data:")

    # Check historic records for additional pids
    logging.info("\t1. Look for missing PIDs' patterns on historic data")

    existing_patterns = os.listdir(DIR_OUT / "patterns_current")
    existing_pids_stops = [re.sub("[^0-9]", "", p).zfill(5) for p in existing_patterns]
    existing_pids_segments = existing_pids_stops.copy()

    new_pids = []

    if not os.path.exists(f"{DIR_OUT}/patterns_historic"):
        os.makedirs(f"{DIR_OUT}/patterns_historic")

    historic_gtfs_shapes = os.listdir(DIR_INP)

    for snapshot in historic_gtfs_shapes:
        """
         logging.debug(f"\tChecking PIDs in {snapshot}")
        """



        # Check that file is valid (a parquet file)
        if not snapshot.endswith(".parquet"):
            continue

        if "segment" in snapshot:
            #process segments
            shape_df = pd.read_parquet(DIR_INP / snapshot)

            # Get unique pids
            snapshot_pids = list(shape_df["pid"].unique())

            # Identify any pid missing from those already processed
            missing_pids = [p for p in snapshot_pids if p not in existing_pids_segments]
            logging.debug(f"\t\tMissing PIDs found: {missing_pids}")

            for pid in missing_pids:
                logging.debug(f"\t\t\tProcessing PID {pid}")
                # Process pid if missing and store shapefile
                df_pid = shape_df[shape_df["pid"] == pid]
                df_segment = convert_to_geometries(df_pid, "segment")
                write_patterns(pid, df_segment, 'segment', DIR_OUT / "patterns_historic")

                existing_pids_segments.append(pid)
                new_pids.append(pid)
        
        if "stop" in snapshot:
            #process segments
            stop_df = pd.read_parquet(DIR_INP / snapshot)

            # Get unique pids
            snapshot_pids = list(stop_df["pid"].unique())

            # Identify any pid missing from those already processed
            missing_pids = [p for p in snapshot_pids if p not in existing_pids_stops]
            logging.debug(f"\t\tMissing PIDs found: {missing_pids}")

            for pid in missing_pids:
                logging.debug(f"\t\t\tProcessing PID {pid}")
                # Process pid if missing and store shapefile
                df_pid = stop_df[stop_df["pid"] == pid]
                df_segment = convert_to_geometries(df_pid, "stop")
                write_patterns(pid, df_segment, 'stop', DIR_OUT / "patterns_historic")

                existing_pids_stops.append(pid)
                new_pids.append(pid)


    logging.info(f"\tNew patterns added for PIDs: {new_pids}")


if __name__ == "__main__":
    main()
