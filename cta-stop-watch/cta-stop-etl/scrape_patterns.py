import os
import pandas as pd
import geopandas as gpd
from dotenv import load_dotenv
import requests
import json
from shapely import LineString

load_dotenv()

M_TO_FT = 3.280839895
BUFFER_DIST = 50.0

# Register CTA API key to scrape bus data
# os.environ['CTA_API_KEY'] = 'user_api_key'


def save_pattern_api(pid: str) -> bool:
    """
    Takes a route pattern from the CTA API (in lat, lon format), and converts
    it into route polygones that can be used to asses if a bus is inside it's
    route and identify which are the clostests bus stops.

    The output is written locally as parquet files for every pid, the files
    are then manually moved to a box folder.

    Input:
        - pid (str): The pattern id to call from the CTA API

    Returns:
        - A boolean  indicating if a data frame for the segment was created

    Outputs:
    - Parquet file with segments as points: out/pattern/pid_{pid}_stop.parquet
    - Parquet file with segments as buffers: "out/pattern/pid_{pid}_segment.parquet"

    """

    # Make call to API for given pid and obtain pattern point data
    url = f"http://www.ctabustracker.com/bustime/api/v2/getpatterns?format=json&key={os.environ['CTA_API_KEY']}&pid={pid}"
    response = requests.get(url)
    pattern = json.loads(response.content)

    if "error" in pattern["bustime-response"]:
        return False

    # Extract pattern info and parse it as a geodataframe with point geometries
    pattern_df = pd.DataFrame(pattern["bustime-response"]["ptr"][0]["pt"])
    pattern_df = gpd.GeoDataFrame(
        pattern_df,
        geometry=gpd.GeoSeries.from_xy(
            x=pattern_df.loc[:, "lon"], y=pattern_df.loc[:, "lat"], crs="EPSG:4326"
        ),
    )

    pattern_df = pattern_df.sort_values(by="seq")

    # Each pair of points consitutes a segment, assign id for future grouping
    pattern_df.loc[:, "segment"] = range(0, len(pattern_df))

    # Build line geometries with each segment (pair of points)
    segments = list(range(0, len(pattern_df)))
    geometries = []

    for segment_id, segment_data in pattern_df.iterrows():
        # The first bus stop is stored as a point geometry instead of line
        if segment_id == 0.0:
            geometries.append(segment_data["geometry"])
            continue
        previous_point = pattern_df.iloc[segment_id - 1]["geometry"]
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

    # Convert geometry vars to strings to preserve info after parquet
    # compression (geopandas raises a warning, remind to convert back to geometry)
    pattern_df["geometry"] = pattern_df["geometry"].astype("string")
    segment_df["geometry"] = segment_df["geometry"].astype("string")
    segment_df["ls_geometry"] = segment_df["ls_geometry"].astype("string")

    print(
        f"Writing out/pattern/pid_{pid}_stop.parquet and out/pattern/pid_{pid}_segment.parquet"
    )
    pattern_df.to_parquet(f"out/pattern/pid_{pid}_stop.parquet")
    segment_df.to_parquet(f"out/pattern/pid_{pid}_segment.parquet")

    return True


if __name__ == "__main__":

    PID_DIR = "out/pids"
    for pid_file in os.listdir(PID_DIR):
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
        save_pattern_api(pid)
