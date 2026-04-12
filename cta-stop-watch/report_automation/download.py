# Import libraries ------------------------------------------------------------
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

# Constants -------------------------------------------------------------------

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


# Functions --------------------------------------------------------------------


def get_date_range(start: date, end: date, delta: timedelta):
    """
    Yielding function to loop over a desired range of days.

    Args:
        start (date): Desired start date of range
        end (date): Desired end date of range
        delta (timedelta): Delta for days included in range (i.e. 1 day)

    Returns:
        date
    """
    cur_date = start
    while cur_date < end:
        yield cur_date
        cur_date += delta


def download_full_day_csv_to_parquet(
    start: date, end: date, delta: timedelta
) -> tuple[bool, bool]:
    """
    Download full day data from the CTA API and save as parquet

    Args:
        start (date): Desired start date of time range for trips to be processed
        end (date): Desired end date of range for trips to be processed
        delta (timedelta): Delta for days included in range (i.e. 1 day)

    Returns:
        (bool, bool)
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


def query_cta_api(pid: str, out_path: str) -> bool:
    """
    Query the CTA API for a route pattern and save it locally. If the pattern
    already exists and has changed, the old version is archived to
    patterns_historic/ before being overwritten.

    Args:
        pid (str): The pattern id to call from the CTA API
        out_path (str): Path to the patterns_raw/ directory

    Returns:
        bool: True if a valid pattern was saved, False if the API returned an error
    """
    load_dotenv()
    BUS_API_KEY = os.environ["BUS_API_KEY"]

    url = f"http://www.ctabustracker.com/bustime/api/v2/getpatterns?format=json&key={BUS_API_KEY}&pid={pid}"
    response = requests.get(url)
    pattern = json.loads(response.content)

    if "error" in pattern["bustime-response"] or "ptr" not in pattern["bustime-response"]:
        process_logger.debug(
            f"API returned no pattern for PID {pid}: "
            f"{pattern['bustime-response'].get('error', 'no ptr key')}"
        )
        return False

    df_new = pd.DataFrame(pattern["bustime-response"]["ptr"][0]["pt"])

    raw_path = f"{out_path}/pid_{pid}_raw.parquet"
    historic_dir = os.path.join(os.path.dirname(out_path), "patterns_historic")

    if os.path.exists(raw_path):
        df_existing = pd.read_parquet(raw_path)

        def stop_sequence(df):
            return df[df["typ"] == "S"]["stpid"].dropna().tolist()

        if stop_sequence(df_existing) != stop_sequence(df_new):
            os.makedirs(historic_dir, exist_ok=True)
            today = date.today().strftime("%Y-%m-%d")
            archive_path = f"{historic_dir}/pid_{pid}_raw_{today}.parquet"
            df_existing.to_parquet(archive_path)
            process_logger.info(
                f"PID {pid} pattern changed — archived old version to {archive_path}"
            )
        else:
            process_logger.debug(f"PID {pid} pattern unchanged")
            return True

    df_new.to_parquet(raw_path)
    return True
