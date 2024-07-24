import pathlib
import os
import re
import json
from datetime import datetime
import logging
import shutil


def create_config():
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

    config["EXISTING_PATTERNS"] = pids

    DIR_b = pathlib.Path(__file__).parent / "data/raw_trips"

    dates_file = [x.split(".")[0] for x in os.listdir(DIR_b)]

    dates_file.sort(key=lambda date: datetime.strptime(date, "%Y-%m-%d"))

    MAX_DATE = dates_file[-1]

    config["MAX_DATE"] = MAX_DATE

    process_logger.info(
        f"Updating config file. New max date: {MAX_DATE}. Now have {len(pids)} patterns."
    )

    with open("config.json", "w") as file:
        json.dump(config, file)


def clear_staging(folders: list, files: list):
    """
    Remove all files/ folders from the staging directory
    """

    DIR = pathlib.Path(__file__).parent / "data/staging"

    for folder in folders:
        # remove folder
        if os.path.isdir(DIR / folder):
            shutil.rmtree(DIR / folder)

        # recreate empty folder for next time
        os.makedirs(DIR / folder)

    for file in files:
        if os.path.isfile(DIR / file):
            os.remove(DIR / file)


formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")


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
