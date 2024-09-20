import os
import pandas as pd
from pandas import DataFrame
import geopandas as gpd
from geopandas import GeoDataFrame, GeoSeries
from dotenv import load_dotenv
import requests
import json
from shapely import LineString
from time import time

load_dotenv()

M_TO_FT = 3.280839895
BUFFER_DIST = 50.0

# Register CTA API key to scrape bus data 
# os.environ['CTA_API_KEY'] = 'user_api_key'


def buffer_wgs84_m(geometry: GeoSeries, metres: float):
    return

def append_skipped_pids(pid: str):
    with open("skipped_pids.txt", "+a") as f:
        f.write(pid)
        f.write("\n")

def process_pattern(pid: str, pid_df: GeoDataFrame):

    if not save_pattern_api(pid):
        print(f"skipping {pid=}")
        print(pid_df.iloc[-1].tmstmp)
        append_skipped_pids(pid)
        return False

    pattern_segments = gpd.read_parquet(f"out/pattern/pid_{pid}_segment.parquet")
    pattern_stops = gpd.read_parquet(f"out/pattern/pid_{pid}_stop.parquet")
    max_seg = pattern_segments.segments.max()

    print(f"{pid_df.shape=}")
    # Exlcude pings that are far away
    pid_df = pid_df.sjoin(pattern_segments, how="inner", predicate="within")
    print(f"{pid_df.shape=}")
    t = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
    print(pid_df.loc[:, "unique_trip_vehicle_day"].unique().shape[0])
    tcnt = 0
    scnt = 0
    return True
    for tno, (uid, tripdf) in enumerate(pid_df.groupby("unique_trip_vehicle_day")):
        start = time()
        tripdf = prep_trip(tripdf, pattern_df, max_seg)
        t[0] += time() - start
        tcnt += 1
        for i in range(tripdf.shape[0] - 1):
            start_seg = time()
            segment_pair = tripdf.iloc[i : i + 2]
            if (segment_pair.loc[:, "segments"].unique().shape[0] > 1) and (
                segment_pair.iloc[0].loc["segments"]
                < segment_pair.iloc[1].loc["segments"]
            ):
                inner_segments = segment_polygon.loc[
                    (
                        segment_polygon.loc[:, "segments"]
                        > segment_pair.iloc[0].loc["segments"]
                    )
                    & (
                        segment_polygon.loc[:, "segments"]
                        < segment_pair.iloc[1].loc["segments"]
                    )
                ]
                time_bw_segments = segment_pair.loc[:, "tmstmp"].diff().iloc[1]
                dist_bw_segments = (
                    inner_segments.loc[:, "length_ft"].sum()
                    + segment_pair.iloc[0].loc["length_for_end_cur_segment"]
                    + segment_pair.iloc[1].loc["length_to_last_segment"]
                )
                time_per_ft = time_bw_segments / dist_bw_segments
                # print(f"{time_bw_segments=}")
                # print(f"{dist_bw_segments=}")
                # print(f"{inner_segments=}")
                # print(f'{segment_pair.loc[:,["tmstmp", "segments"]]=}')
                segment_polygon.loc[
                    segment_pair.iloc[0].loc["segments"], "time_spent_in_segment"
                ] = (
                    segment_pair.iloc[0].loc["length_for_end_cur_segment"] * time_per_ft
                )
                segment_polygon.loc[
                    segment_pair.iloc[0].loc["segments"], "occurences_in_segment"
                ] += 1

                segment_polygon.loc[
                    segment_pair.iloc[1].loc["segments"], "time_spent_in_segment"
                ] = (segment_pair.iloc[1].loc["length_to_last_segment"] * time_per_ft)
                inner_segments.loc[:, "time_spent_in_segment"] = (
                    inner_segments.loc[:, "length_ft"] * time_per_ft
                )
                for i, row in inner_segments.iterrows():
                    segment_polygon.loc[
                        segment_polygon.loc[:, "segments"] == row["segments"],
                        "time_spent_in_segment",
                    ] += row["time_spent_in_segment"]
                    segment_polygon.loc[
                        segment_polygon.loc[:, "segments"] == row["segments"],
                        "occurences_in_segment",
                    ] += 1

                t[1] += time() - start_seg
                scnt += 1

    t[0] /= tcnt
    t[1] /= scnt
    print(t)
    # segment_polygon.to_parquet(f"out/pattern/pid_{pid}_segment.parquet")
    # pattern_grp.to_parquet(f"out/processed/pid_{pid}.parquet")


if __name__ == "__main__":
    # df = pd.read_parquet(f"out/6.parquet")
    # df.loc[:, "pid"] = df.loc[:, "pid"].astype("float").astype("int")
    #
    # df.loc[:, "unique_id"] = df.apply(
    #     lambda x: f"{x['rt']}_{x['pid']}_{x['origtatripno']}_{x['tatripid']}_{x['vid']}_{x['data_date']}",
    #     axis=1,
    # )
    # for pid, pattern_grp in df.groupby("pid"):
    #     process_pattern(pid, pattern_grp)

    PID_DIR = "out/pids"

    for pid_file in os.listdir(PID_DIR):
        # print(f"{PID_DIR}/{pid_file}")
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
        print(pid_df)
        if process_pattern(pid, pid_df):
            break
