from download import full_download, extract_routes, query_cta_api
from process_patterns import process_patterns
from calculate_stop_time import calculate_patterns
from utils import create_config, clear_staging, process_logger

from datetime import date, timedelta
import polars as pl
import pandas as pd
import duckdb
import json
import os

with open("config.json", "r") as file:
    config = json.load(file)

MAX_DATE = config["MAX_DATE"]
EXISTING_PATTERNS = config["EXISTING_PATTERNS"]

STAGING_PATH = "data/staging"
today_minus_one = str(date.today() - timedelta(days=1))
today = str(date.today())


def update_data(today):

    # TODO update file path
    full_download(MAX_DATE, today)
    # covert to by pattern
    extract_routes()


def update_patterns():
    # get all patterns in the database from new data
    new_trip_pids = pd.read_parquet(f"{STAGING_PATH}/all_pids_list.parquet")

    # get all processed patterns (#EXISITNG_PATTERNS)

    # compare the two if there are new patterns, download from api and add them to the database
    new_patterns = list(
        set(new_trip_pids["pid"].astype(str).tolist()) - set(EXISTING_PATTERNS)
    )
    bad_pids = []
    if len(new_patterns) > 0:
        # for any new patterns, try to download from the api

        for pid in new_patterns:
            try:
                query_cta_api(pid, "data/patterns/patterns_raw")
            except Exception as e:
                print(f"Error downloading pattern {pid}: {e}")
                bad_pids.append(pid)

    # process new patterns
    new_raw_patterns = list(set(new_patterns) - set(bad_pids))
    process_patterns(new_raw_patterns)

    process_logger.info(
        f""" Found {len(new_patterns)} new pattern(s) in data \n
             Downloaded {len(new_patterns) - len(bad_pids)} new patterns \n
             Issues with {len(bad_pids)} pattern(s): {bad_pids}
        """
    )

    return new_trip_pids


def trip_to_day():
    """
    convert the current trip data into day data
    """

    # combine all the pattern trip files into one

    all_data = """COPY
    (SELECT
        *
    FROM read_parquet('data/staging/trips/*.parquet'))
    TO 'data/staging/trips/combined.parquet'
    (FORMAT 'parquet');"""

    duckdb.execute(all_data)

    dates = """SELECT
        strftime(bus_stop_time, '%Y-%m-%d') as day,
    FROM read_parquet('data/staging/trips/*.parquet')
    group by 1"""

    dates_df = duckdb.execute(dates).df()

    all_trips = pl.scan_parquet("data/staging/trips/combined.parquet")

    date_fmt = "%F"

    for day in dates_df["day"].tolist():
        by_day = all_trips.filter(
            pl.col("bus_stop_time").dt.date()
            == pl.lit(day).str.strptime(pl.Date, format=date_fmt)
        )
        if os.path.exists(f"data/processed_by_day/{day}.parquet"):

            # combine files
            # TODO make sure this is working properly
            combine = f"""COPY
                        (SELECT  *
                        FROM     read_parquet('data/processed_by_day/{day}.parquet')
                        UNION ALL
                        SELECT  *
                        from    by_day
                        ) TO 'data/processed_by_day/{day}.parquet'
                        (FORMAT 'parquet');"""

            duckdb.execute(combine)

        else:
            by_day.sink_parquet(f"data/processed_by_day/{day}.parquet")


def process_new_trips():

    # 1 download data from ghost buses from max_date to today
    # saves currently to data/raw_trips
    # also saves staging in staging/days, staging/pids
    process_logger.info(
        f"Trying to download ghost bus data from data from {MAX_DATE} to {today_minus_one}"
    )
    update_data(today_minus_one)

    # 2 check if there are new patterns in the new data
    # download raw patterns to data/patters/patterns_raw
    # process the raw patterns and save them to data/patterns/patterns_current
    process_logger.info(
        f"Attempting to find and download any missing patterns from new data"
    )
    update_patterns()

    all_pids_df = pd.read_parquet(f"{STAGING_PATH}/all_pids_list.parquet")

    # 3 calculate the stop time for all the patterns
    # puts the processed trips by pattern in staging/trips

    calculate_patterns(all_pids_df["pid"].astype(str).tolist())

    # recreate updated config file

    create_config()

    # clear staging data (days and pids)
    clear_staging(folders=["days", "pids"], files=["current_days_download.parquet"])
