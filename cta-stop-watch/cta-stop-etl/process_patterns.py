import polars as pl 
import pandas as pd
import geopandas as gpd
from shapely import LineString
import pathlib
import logging
from pyproj import Proj, CRS

## CONSTANTS
M_TO_FT = 3.280839895
BUFFER_DIST = 50
PID_DIR = pathlib.Path(__file__).parent / "out"

PROJ_4326 = CRS("epsg:4326")
PROJ_26971 = CRS("epsg:26971")

def load_raw_pattern(pid: str) -> pd.DataFrame | bool:
    try:
        df_raw = pd.read_parquet(f"{PID_DIR}/patterns_raw/pid_{pid}_raw.parquet")
        return True, df_raw
    except FileNotFoundError:
        return False, False

def convert_to_geometries(df_raw: pd.DataFrame, pid: str, write = True) -> bool:
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
            x=df_raw.loc[:, "lon"], y=df_raw.loc[:, "lat"], crs=PROJ_4326
        ),
    )

    df_pattern = df_pattern.sort_values(by="seq")

    # Each pair of points consitutes a segment, assign id for future grouping
    df_pattern.loc[:, "segment"] = range(0, len(df_pattern))

    # Build line geometries with each segment (pair of points)
    segments = list(range(0, len(df_pattern)))
    geometries = []

    for segment_id, segment_data in df_pattern.iterrows():
        # The first bus stop is stored as a point geometry instead of line
        if segment_id == 0.0:
            geometries.append(segment_data["geometry"])
            continue
        previous_point = df_pattern.iloc[segment_id - 1]["geometry"]
        geometry = LineString([previous_point, segment_data["geometry"]])
        geometries.append(geometry)

    # Change projection and units so distance and time can be computed (in feet)
    df_segment = gpd.GeoDataFrame(
        data={"segments": segments}, geometry=geometries, crs=PROJ_4326
    ).sort_values("segments")
    df_segment.loc[:, "length_ft"] = (
        df_segment.geometry.to_crs(PROJ_26971).length * M_TO_FT
    )
    df_segment.loc[:, "ls_geometry"] = df_segment.geometry
    df_segment.geometry = (
        df_segment.geometry.to_crs(PROJ_26971).buffer(BUFFER_DIST).to_crs(PROJ_4326)
    )

    # create unique id for each stop on the pattern
    df_pattern["p_stp_id"] = str(pid) + "-" + df_pattern["stpid"]

    if write: 
        logging.debug(
            f"Writing out/patterns_current/pid_{pid}_stop.parquet and out/patterns_current/pid_{pid}_segment.parquet"
        )
        df_pattern.to_parquet(f"{PID_DIR}/patterns_current/pid_{pid}_stop.parquet")
        df_segment.to_parquet(f"{PID_DIR}/patterns_current/pid_{pid}_segment.parquet")

        return True
    return df_segment


def process_patterns(pids: list):
    """
    process all the patterns
    """
    bad_pids = []
    for pid in pids:

        found, df_raw = load_raw_pattern(pid)
        if not found:
            bad_pids.append(pid)
            continue
        else:
            convert_to_geometries(df_raw, pid)
            print(f"Succes in converting pattern {pid} to geometry")

    if len(bad_pids) > 0:
        print(
            f"Missing {len(bad_pids)} PIDs from ghost bus data that we do not have. List here: {bad_pids}"
        )


if __name__ == "__main__": 
    process_patterns(["14103"])