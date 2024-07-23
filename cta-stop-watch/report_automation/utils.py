import pathlib
import os
import re
import json
from datetime import datetime


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

    DIR_b = pathlib.Path(__file__).parent / "data/raw_trips/raw_by_day"

    dates_file = [x.split(".")[0] for x in os.listdir(DIR_b)]

    dates_file.sort(key=lambda date: datetime.strptime(date, "%Y-%m-%d"))

    MAX_DATE = dates_file[-1]

    config["MAX_DATE"] = MAX_DATE

    with open("config.json", "w") as file:
        json.dump(config, file)


def clear_staging():
    pass
