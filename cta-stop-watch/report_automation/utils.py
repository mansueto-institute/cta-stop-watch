import pathlib
import os
import re
import json
from datetime import datetime, timedelta
import logging
import shutil
import duckdb
import polars as pl

DIR = pathlib.Path(__file__).parent / "data"


def create_config(test: bool = False):
    """
    Create a configuration file from a dictionary
    """

    config = {}
    pids = []

    DIR_p = pathlib.Path(__file__).parent / "data/patterns/patterns_raw"

    for pid_file in os.listdir(DIR_p):
        if not pid_file.endswith(".parquet"):
            continue

        numbers = re.findall(r"\d+", pid_file)
        pid = numbers[0]
        pids.append(pid)

    DIR_b = pathlib.Path(__file__).parent / "data/raw_trips"

    dates_file = [x.split(".")[0] for x in os.listdir(DIR_b) if x.endswith(".parquet")]

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


def clear_staging(folders: list = [], files: list = []):
    """
    Remove all files/ folders from the indicated directory
    """

    DIR = pathlib.Path(__file__).parent / "data"

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


def setup_logger(name, log_file, level=logging.INFO):
    """To setup as many loggers as you want
    # from https://stackoverflow.com/questions/11232230/logging-to-two-files-with-different-settings
    """

    handler = logging.FileHandler(log_file)
    handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)

    return logger


formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
# first file logger
process_logger = setup_logger("process", "process.log")

# second file logger
metrics_logger = setup_logger("metrics", "metrics.log")
