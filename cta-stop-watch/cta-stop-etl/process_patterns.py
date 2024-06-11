import os
import sys
import pandas as pd
import geopandas as gpd
from shapely import LineString
import pathlib

## CONSTANTS
M_TO_FT = 3.280839895
BUFFER_DIST = 50


def convert_to_geometries(pid: str) -> bool:
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

    # load in raw pattern data
    PID_DIR = pathlib.Path(__file__).parent / "out"

    try:
        df_raw = pd.read_parquet(f"{PID_DIR}/patterns_raw/pid_{pid}_raw.parquet")
    except FileNotFoundError:
        print(f"Pattern {pid} not found")
        return False

    # Convert into geodata with projection for Chicago (EPSG 4326)
    df_pattern = gpd.GeoDataFrame(
        df_raw,
        geometry=gpd.GeoSeries.from_xy(
            x=df_raw.loc[:, "lon"], y=df_raw.loc[:, "lat"], crs="EPSG:4326"
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
    # comment out for v2 to retain overlapping shapes

    # for i in range(1, segment_df.shape[0]):
    #     segment_df.loc[i, "geometry"] = segment_df.iloc[i].geometry.difference(
    #         segment_df.iloc[0:i].geometry.unary_union
    #     )

    segment_df.loc[:, "time_spent_in_segment"] = pd.to_timedelta(0)
    segment_df.loc[:, "occurences_in_segment"] = 0

    print(
        f"Writing out/patterns/pid_{pid}_stop.parquet and out/patterns/pid_{pid}_segment.parquet"
    )
    df_pattern.to_parquet(f"{PID_DIR}/patterns/pid_{pid}_stop.parquet")
    segment_df.to_parquet(f"{PID_DIR}/patterns/pid_{pid}_segment.parquet")

    return True


if __name__ == "__main__":

    DIR = pathlib.Path(__file__).parent / "out"

    all_pids = os.listdir(f"{DIR}/pids")

    # Run process for all of the files
    if len(sys.argv) == 1:
        pids = all_pids
    # Run process just for specified pid
    else:
        pids = [sys.argv[1]]

    if not os.path.exists(f"{DIR}/patterns"):
        os.makedirs(f"{DIR}/patterns")

    for pid_file in pids:
        pid = pid_file.replace(".parquet", "")
        print(f"Processing pattern for PID: {pid}")
        convert_to_geometries(pid)
