import json
import os
from datetime import date, timedelta
from pathlib import Path

import duckdb
import pandas as pd
import polars as pl
import requests
from dotenv import load_dotenv
from utils import process_logger

STAGING_PATH = "data/staging"
RAW_PATH = "data/raw_trips/"

dtype_map = {
    "vid": pl.UInt32,
    "tmstmp": pl.Utf8,
    "lat": pl.Float64,
    "lon": pl.Float64,
    "hdg": pl.UInt32,
    "pid": pl.Float64,
    "rt": pl.Utf8,
    "des": pl.Utf8,
    "pdist": pl.Utf8,
    "dly": pl.Boolean,
    "tatripid": pl.Utf8,
    "origtatripno": pl.Utf8,
    "tablockid": pl.Utf8,
    "zone": pl.Utf8,
    "scrape_file": pl.Utf8,
    "data_time": pl.Utf8,
    "data_hour": pl.Utf8,
    "data_date": pl.Utf8,
}


def get_date_range(start: date, end: date, delta: timedelta):
    cur_date = start
    while cur_date < end:
        yield cur_date
        cur_date += delta


def download_full_day_csv_to_parquet(start: date, end: date, delta: timedelta):
    """
    Download full day data from the CTA API and save as parquet
    """
    URL_HEAD = "gs://miurban-dj-public/cta-stop-watch/full_day_data/"

    # TODO update paths
    out_staging_path = f"{STAGING_PATH}/days/"

    os.makedirs(RAW_PATH, exist_ok=True)

    failed = []
    success = []

    for day in get_date_range(start, end, delta):
        day_f = day.strftime("%Y-%m-%d")
        day_csv = day_f + ".csv"
        day_parquet = day_f + ".parquet"
        url_day = URL_HEAD + day_csv
        if Path(RAW_PATH + day_parquet).exists():
            process_logger.info(f"Skipping {day_f} as it already exists")
            continue

        try:
            df = pl.read_csv(url_day, dtypes=dtype_map)
            success.append(day_f)
            # save file
            df.write_parquet(RAW_PATH + day_parquet)

            # save for staging
            df.write_parquet(out_staging_path + day_parquet)
        except Exception as e:
            process_logger.error(f"Failed to download {day_f}: {e}")
            failed.append(day_f)

    return success, failed


def save_partitioned_parquet(in_folder, out_file: str):
    """
    create one big parquet file with a unique trip id for all downloaded days
    """
    cmd_number = f"""COPY
    (SELECT
        *,
        CONCAT(
            rt, pid, tatripid, vid, data_date
        ) AS unique_trip_vehicle_day
    FROM read_parquet('{in_folder}/*.parquet'))
    TO '{out_file}'
    (FORMAT 'parquet');"""
    duckdb.execute(cmd_number)


def full_download(start: str = "2023-1-1", end: str = "2024-12-31"):
    """
    download full days from start to end, then save them and log results.
    """

    start = start.split("-")
    end = end.split("-")
    start = date(year=int(start[0]), month=int(start[1]), day=int(start[2]))
    end = date(year=int(end[0]), month=int(end[1]), day=int(end[2]))

    delta = timedelta(days=1)
    success, failed = download_full_day_csv_to_parquet(start, end, delta)

    # log success and failed TODO
    process_logger.info(f"Downloaded {len(success)} day(s): {success}")
    process_logger.info(f"Issues with {len(failed)} day(s): {failed}")

    if len(success) == 0:
        process_logger.info("No days downloaded. Exiting")
        return False

    save_partitioned_parquet(
        f"{STAGING_PATH}/days", f"{STAGING_PATH}/current_days_download.parquet"
    )

    return success


def extract_list_pids():
    """
    get list of all pids that were downloaded
    """
    df = pl.scan_parquet(f"{STAGING_PATH}/current_days_download.parquet")
    df_routes = df.select(pl.col("pid").cast(pl.Int32, strict=False).unique())
    df_routes.collect().write_parquet(f"{STAGING_PATH}/all_pids_list.parquet")


def extract_pid(pid: int):
    """
    convert days into one file per pid
    """
    df = pl.scan_parquet(f"{STAGING_PATH}/current_days_download.parquet")
    df_route = df.filter(pl.col("pid").cast(pl.Int32, strict=False) == pid)
    df_route.sink_parquet(f"{STAGING_PATH}/pids/{pid}.parquet")


def extract_routes():
    """
    Grab all pids from current data download and separate them into individual files for each pid

    """

    extract_list_pids()
    all_pids_df = pl.read_parquet(f"{STAGING_PATH}/all_pids_list.parquet")

    for row in all_pids_df.iter_rows(named=True):
        extract_pid(row["pid"])


def query_cta_api(pid: str, out_path) -> bool | pd.DataFrame:
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

    load_dotenv()

    BUS_API_KEY = os.environ["BUS_API_KEY"]

    if os.path.exists(out_path + "/patterns_raw/pid_" + pid + "_raw.parquet"):
        process_logger.info(f"Skipping PID {pid} as it already exists")

    url = f"http://www.ctabustracker.com/bustime/api/v2/getpatterns?format=json&key={BUS_API_KEY}&pid={pid}"
    response = requests.get(url)
    pattern = json.loads(response.content)

    # if "error" in pattern["bustime-response"]:
    #     logging.debug("\t\t\t Skiping PID {pid}")
    #     return False

    df_pattern = pd.DataFrame(pattern["bustime-response"]["ptr"][0]["pt"])

    df_pattern.to_parquet(f"{out_path}/pid_{pid}_raw.parquet")

    return True
