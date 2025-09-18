# Libraries -------------------------------------------------------------------

import json
import logging
import os
import pathlib
import re
from datetime import datetime, timedelta

import duckdb
import polars as pl

# Constants -------------------------------------------------------------------

# Paths
DIR = pathlib.Path(__file__).parent / "data"
DIR_PATTERNS = DIR / "patterns/patterns_raw"
DIR_TRIPS = DIR / "raw_trips"

# Functions -------------------------------------------------------------------


def create_config(test: bool = False) -> None:
    """
    Create a configuration file from a dictionary.

    Args:
        test (bool): flag indicating if running as test

    Returns:
        None
    """

    config = {}
    pids = []

    for pid_file in os.listdir(DIR_PATTERNS):
        if not pid_file.endswith(".parquet"):
            continue

        numbers = re.findall(r"\d+", pid_file)
        pid = numbers[0]
        pids.append(pid)

    dates_file = [
        x.split(".")[0] for x in os.listdir(DIR_TRIPS) if x.endswith(".parquet")
    ]
    dates_file.sort(key=lambda date: datetime.strptime(date, "%Y-%m-%d"))

    try:
        MAX_DATE = dates_file[-1]
    except IndexError:
        MAX_DATE = input("No existing files. Enter start date as string YYYY-MM-DD: ")

    if test:
        # max date is yesterday
        MAX_DATE = str(datetime.today().date() - timedelta(days=1))

    config["MAX_DATE"] = MAX_DATE
    config["EXISTING_PATTERNS"] = pids

    process_logger.info(
        f"Updating config file. New max date: {MAX_DATE}. Now have {len(pids)} patterns."
    )

    with open("config.json", "w") as file:
        json.dump(config, file)


def clear_staging(folders: list = [], files: list = []) -> None:
    """
    Remove all files/ folders from the indicated directory

    Args:
        folders (list): Folders with staged files
        files (list): Staged files to remove

    Returns:
        None
    """

    for folder in folders:
        for path, _, files in os.walk(DIR / folder):
            for f in files:
                if f != ".gitkeep":
                    os.remove(os.path.join(path, f))

    for file in files:
        if os.path.isfile(DIR / file):
            os.remove(DIR / file)


def create_rt_pid_xwalk() -> bool:
    """
    Create a route to pattern id crosswalk called rt_to_pid.parquet
    """

    df = pl.scan_parquet(f"{DIR}/staging/current_days_download.parquet")
    xwalk = df.with_columns(pl.col("pid").cast(pl.Int32).cast(pl.String))

    xwalk.select(pl.col(["rt", "pid"])).unique(["rt", "pid"]).sink_parquet(
        f"{DIR}/staging/rt_to_pid_new.parquet"
    )

    combine = f"""COPY (SELECT * 
                        FROM read_parquet('{DIR}/rt_to_pid.parquet')
                        UNION DISTINCT
                        SELECT * 
                        FROM read_parquet("{DIR}/staging/rt_to_pid_new.parquet")
                        ) 
                        TO '{DIR}/rt_to_pid.parquet' (FORMAT 'parquet');"""
    duckdb.execute(combine)

    return True


def setup_logger(name: str, log_file: str, level=logging.DEBUG) -> logging.Logger:
    """
    To setup as many loggers as you want.
    Source: https://stackoverflow.com/questions/11232230/logging-to-two-files-with-different-settings

    Args:
        name (str): Name of logger (e.g. my_logger)
        log_file (str): Name of logger file (includes file extension, e.g. my_logger.log)
        level (logging levelname): Desired lowest log level to record

    Returns:
        logging.Logger
    """

    handler = logging.FileHandler(log_file)
    handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)

    return logger


# Loggers ---------------------------------------------------------------------

formatter = logging.Formatter(
    "[%(asctime)s] (%(levelname)5s: %(filename)s, %(lineno)-4d): %(message)s",
    "%Y-%m-%d, %H:%M:%S",
)

# first file logger
process_logger = setup_logger("process", "process.log")

# second file logger
metrics_logger = setup_logger("metrics", "metrics.log")

# End -------------------------------------------------------------------------
