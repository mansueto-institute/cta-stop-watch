import os
import sys
import pandas as pd
import geopandas as gpd
from dotenv import load_dotenv
import requests
import json
from shapely import LineString
import pathlib
import re

load_dotenv()

M_TO_FT = 3.280839895
BUFFER_DIST = 50.0

# Register CTA API key to scrape bus data
os.environ["CTA_API_KEY"] = "FMdePKG2y5dbVjy25RYUWMY2R"


def query_cta_api(pid: str) -> pd.DataFrame:
    """
    Takes a route pattern ID and queries the CTA API to get the raw pattern
    data (in lat, lon format) and returns a standardized data frame for further
    cleaning.

    Input:
        - pid (str): The pattern id to call from the CTA API

    Output:
        - pattern (data frame): A data frame with standardized names

    """
    # Make call to API for given pid and obtain pattern point data
    url = f"http://www.ctabustracker.com/bustime/api/v2/getpatterns?format=json&key={os.environ['CTA_API_KEY']}&pid={pid}"
    response = requests.get(url)
    pattern = json.loads(response.content)
    df_pattern = pd.DataFrame(pattern["bustime-response"]["ptr"][0]["pt"])

    if "error" in pattern["bustime-response"]:
        return False

    return df_pattern


def convert_to_geometries(df_pattern: pd.DataFrame) -> bool:
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
        df_pattern,
        geometry=gpd.GeoSeries.from_xy(
            x=df_pattern.loc[:, "lon"], y=df_pattern.loc[:, "lat"], crs="EPSG:4326"
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
    segment_df = gpd.GeoDataFrame(
        data={"segments": segments}, geometry=geometries, crs="EPSG:4326"
    ).sort_values("segments")
    segment_df.loc[:, "length_ft"] = (
        segment_df.geometry.to_crs("EPSG:26971").length * M_TO_FT
    )
    segment_df.loc[:, "ls_geometry"] = segment_df.geometry
    segment_df.geometry = (
        segment_df.geometry.to_crs("EPSG:26971").buffer(BUFFER_DIST).to_crs("EPSG:4326")
    )

    # To avoid overlapping of shapes,
    for i in range(1, segment_df.shape[0]):
        segment_df.loc[i, "geometry"] = segment_df.iloc[i].geometry.difference(
            segment_df.iloc[0:i].geometry.unary_union
        )

    segment_df.loc[:, "time_spent_in_segment"] = pd.to_timedelta(0)
    segment_df.loc[:, "occurences_in_segment"] = 0

    print(
        f"Writing out/pattern/pid_{pid}_stop.parquet and out/pattern/pid_{pid}_segment.parquet"
    )
    df_pattern.to_parquet(f"out/patterns/pid_{pid}_stop.parquet")
    segment_df.to_parquet(f"out/patterns/pid_{pid}_segment.parquet")

    return True


if __name__ == "__main__":

    PID_DIR = pathlib.Path(__file__).parent / "out/pids"

    all_pids = os.listdir(PID_DIR)

    # Run process for all of the files
    if len(sys.argv) == 1:
        pids = all_pids
    # Run process just for specified pid
    else:
        pattern = re.compile("^" + sys.argv[1] + "\.")
        pids = [f for f in all_pids if pattern.match(f)]

    for pid_file in pids:
        print(f"Reading {PID_DIR}/{pid_file}")
        pid_df = pd.read_parquet(f"{PID_DIR}/{pid_file}")
        pid_df.loc[:, "tmstmp"] = pd.to_datetime(
            pid_df.loc[:, "tmstmp"], format="%Y%m%d %H:%M"
        )
        pid_df = gpd.GeoDataFrame(
            pid_df,
            geometry=gpd.GeoSeries.from_xy(
                x=pid_df.loc[:, "lon"], y=pid_df.loc[:, "lat"], crs="EPSG:4326"
            ),
        )

        pid = pid_file.replace(".0.parquet", "")

        # TODO: Expand implementation to take the patterns either from the
        # CTA API or the archival registry of GTFS
        if True:
            pattern = query_cta_api(pid)

        convert_to_geometries(pattern)
