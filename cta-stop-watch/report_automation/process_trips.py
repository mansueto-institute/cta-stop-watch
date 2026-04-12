# Imports ---------------------------------------------------------------------

from download import full_download, extract_routes, query_cta_api
from process_patterns import process_patterns
from calculate_stop_time import calculate_patterns
from utils import (
    create_config,
    clear_staging,
    process_logger,
    create_rt_pid_xwalk,
    debug_logger,
)

from datetime import date, timedelta, datetime
import polars as pl
import pandas as pd
import duckdb
import json
import os

# Constants -------------------------------------------------------------------

# Paths
STAGING_PATH = "data/staging"

# Debug and logging
DIV_LINE = f"\n{'-'*80}\n"
TEST_PID = "4110"  # Pattern from route 6

# Functions -------------------------------------------------------------------


def update_data(start_date: str, today: str) -> bool:
    """
    Download raw trip data and prepare to process

    Args:
        start_date (str): Starting date of period to be processed
        today (str): Ending date of period to be processed (most correpond to today's date)

    Returns:
        bool
    """

    # TODO update file path
    check = full_download(start_date, today)

    if not check:
        return False
    # covert to by pattern
    extract_routes()

    return True


def update_patterns(EXISTING_PATTERNS: list[str]) -> pd.DataFrame:
    """
    For every PID active in today's GPS data, query the CTA API to ensure the
    stored pattern is current. New PIDs are downloaded fresh; existing PIDs are
    compared against the live API and archived if they have changed.

    Args:
        EXISTING_PATTERNS (list[str]): PIDs already in StopWatch history

    Returns:
        pd.DataFrame: all_pids_list for today's data
    """
    new_trip_pids = pd.read_parquet(f"{STAGING_PATH}/all_pids_list.parquet")
    today_pids = new_trip_pids["pid"].dropna().astype(str).tolist()

    new_pids = set(today_pids) - set(EXISTING_PATTERNS)
    existing_pids = set(today_pids) & set(EXISTING_PATTERNS)

    process_logger.info(
        f"Checking {len(today_pids)} active PIDs: "
        f"{len(new_pids)} new, {len(existing_pids)} existing"
    )

    bad_pids = []
    found_pids = []

    for pid in today_pids:
        if pid == TEST_PID:
            debug_logger.debug(f"Trying to update pattern {TEST_PID}, querying CTA API...")
        try:
            result = query_cta_api(pid, "data/patterns/patterns_raw")
            if result:
                found_pids.append(pid)
            else:
                bad_pids.append(pid)
        except Exception as e:
            print(f"Error downloading pattern {pid}: {e}")
            process_logger.error(f"Error downloading pattern {pid}: {e}")
            debug_logger.error(f"Error downloading pattern {pid}: {e}")
            bad_pids.append(pid)

    # Re-process patterns for any that were successfully fetched (new or updated)
    all_patterns = set(found_pids) | set(EXISTING_PATTERNS)
    process_logger.info(f"Processing {len(all_patterns)} patterns")
    process_patterns(list(all_patterns))

    process_logger.info(
        f"""
        Active PIDs today: {len(today_pids)} ({len(new_pids)} new, {len(existing_pids)} existing)
        Successfully fetched: {len(found_pids)}
        Failed (no pattern from API): {len(bad_pids)} — {bad_pids}
        """
    )

    return new_trip_pids


def trip_to_day() -> None:
    """
    Convert the current trip data into day data

    Args:
        None

    Returns:
        None
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
    Run the process new trip pipeline by executing the following steps:
        1. Download data
        2. Check for new patterns
        3. Calculate stop time for all trips
        4. Make a new config file
        5. Create a new xwalk
        6. Clear staging data
    """

    # SETP 1. Download data from ghost buses from max_date to today
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
        f"{DIV_LINE}DOWNLOAD DATA: \n   Trying to download ghost bus data from data from {start_date} to {today_minus_one}"
    )
    check = update_data(start_date, today)

    if not check:
        create_config()
        return False

    # STEP 2. Check if there are new patterns in the new data
    # download raw patterns to data/patters/patterns_raw
    # process the raw patterns and save them to data/patterns/patterns_current
    process_logger.info(
        "Attempting to find and download any missing patterns from new data"
    )
    update_patterns(EXISTING_PATTERNS)

    process_logger.info(f"{DIV_LINE}PROCESS NEW TRIPS\n")

    all_pids_df = pd.read_parquet(f"{STAGING_PATH}/all_pids_list.parquet")

    # STEP 3. Calculate the stop time for all the patterns
    # puts the processed trips by pattern in staging/trips
    process_logger.info(f"{DIV_LINE}CALCULATE STOP TIME FOR ALL PATTERNS\n")
    calculate_patterns(all_pids_df["pid"].astype(str).tolist())

    # STEP 4. Recreate updated config file
    process_logger.info(f"{DIV_LINE}UPDATE CONFIG FILE AND CROSSWALK\n")
    create_config()

    # STEP 5. Update crosswalk
    create_rt_pid_xwalk()

    # STEP 6. Clear staging data (days and pids, and raw_trips)
    # TODO: Remove flags after succesfully updating data
    debug = True
    if not debug:
        process_logger.info(f"{DIV_LINE}CLEAR STAGING\n")
        clear_staging(
            folders=["staging/days", "staging/pids", "raw_trips"],
            files=["staging/current_days_download.parquet"],
        )
    process_logger.info(f"\n FINISHED PROCESS PIPELINE {DIV_LINE}")


# End -------------------------------------------------------------------------
