from download import full_download, extract_routes, query_cta_api
from process_patterns import process_patterns
from calculate_stop_time import calculate_patterns
from utils import create_config, clear_staging, process_logger, create_rt_pid_xwalk

from datetime import date, timedelta, datetime
import polars as pl
import pandas as pd
import duckdb
import json
import os

# Constants -------------------------------------------------------------------

# Paths
STAGING_PATH = "data/staging"

# Functions -------------------------------------------------------------------


def update_data(start_date: str, today: str) -> bool:
    """
    download raw trip data and prepare to process
    """

    # TODO update file path

    check = full_download(start_date, today)

    if not check:
        return False
    # covert to by pattern
    extract_routes()

    return True


def update_patterns(EXISTING_PATTERNS: list) -> set[str]:
    """
    check for new patterns in the data and download them from CTA API if doesnt exist.
    """
    # get all patterns in the database from new data
    new_trip_pids = pd.read_parquet(f"{STAGING_PATH}/all_pids_list.parquet")

    # get all processed patterns (#EXISITNG_PATTERNS)

    # compare the two if there are new patterns, download from api and add them to the database
    new_patterns = list(
        set(new_trip_pids["pid"].astype(str).tolist()) - set(EXISTING_PATTERNS)
    )
    bad_pids = []
    found_pids = []
    if len(new_patterns) > 0:
        # for any new patterns, try to download from the api

        for pid in new_patterns:
            try:
                query_cta_api(pid, "data/patterns/patterns_raw")
                found_pids.append(pid)
            except Exception as e:
                print(f"Error downloading pattern {pid}: {e}")
                bad_pids.append(pid)

    # process all patterns
    all_patterns = set(found_pids + EXISTING_PATTERNS)
    process_logger.info(f"Processing {len(all_patterns)} patterns")
    process_patterns(list(all_patterns))

    process_logger.info(
        f""" Found {len(new_patterns)} new pattern(s) in data \n
             Downloaded {len(found_pids)} new patterns \n
             Issues with {len(bad_pids)} pattern(s): {bad_pids}
        """
    )

    return new_trip_pids


def trip_to_day() -> None:
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


def process_new_trips(test: bool = False) -> None:
    """
    1. Download data
    2. check for new patterns
    3. calculate stop time for all trips
    4. Make a new config file
    5. create a new xwalk
    6. Clear staging data
    """

    # 1 download data from ghost buses from max_date to today
    # saves currently to data/raw_trips
    # also saves staging in staging/days, staging/pids

    if test:
        create_config(test)

    with open("config.json", "r") as file:
        config = json.load(file)

    MAX_DATE = config["MAX_DATE"]
    EXISTING_PATTERNS = config["EXISTING_PATTERNS"]

    # get today's date and yesterday's date
    today_minus_one = str(date.today() - timedelta(days=1))
    today = str(date.today())

    # first date is MAX_DATE + 1
    MAX_DATE = datetime.strptime(MAX_DATE, "%Y-%m-%d")
    modified_date = MAX_DATE + timedelta(days=1)
    start_date = datetime.strftime(modified_date, "%Y-%m-%d")

    process_logger.info(
        f"Trying to download ghost bus data from data from {start_date} to {today_minus_one}"
    )
    check = update_data(start_date, today)

    if not check:
        create_config()
        return False

    # 2 check if there are new patterns in the new data
    # download raw patterns to data/patters/patterns_raw
    # process the raw patterns and save them to data/patterns/patterns_current
    process_logger.info(
        "Attempting to find and download any missing patterns from new data"
    )
    update_patterns(EXISTING_PATTERNS)

    process_logger.info("Processing new trips")

    all_pids_df = pd.read_parquet(f"{STAGING_PATH}/all_pids_list.parquet")

    # 3 calculate the stop time for all the patterns
    # puts the processed trips by pattern in staging/trips

    calculate_patterns(all_pids_df["pid"].astype(str).tolist())

    # recreate updated config file
    create_config()

    # update crosswalk
    create_rt_pid_xwalk()

    # clear staging data (days and pids, and raw_trips)
    clear_staging(
        folders=["staging/days", "staging/pids", "raw_trips"],
        files=["staging/current_days_download.parquet"],
    )


# End ------------------------------------------------------------------------
