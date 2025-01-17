import os
import pandas as pd
import requests
import json
import pathlib
import logging

from dotenv import load_dotenv

# Load API keys 
load_dotenv()

DIR = pathlib.Path(__file__).parent.parent / "cta-stop-etl/out"

def query_cta_api(pid: str) -> bool | pd.DataFrame:
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
    url = f"http://www.ctabustracker.com/bustime/api/v2/getpatterns?format=json&key={os.environ['CTA_API_KEY']}&pid={pid}"
    response = requests.get(url)
    pattern = json.loads(response.content)

    if "error" in pattern["bustime-response"]:
        logging.debug("\t\t\t Skiping PID {pid}")
        return False

    df_pattern = pd.DataFrame(pattern["bustime-response"]["ptr"][0]["pt"])

    df_pattern.to_parquet(f"{DIR}/patterns_raw/pid_{pid}_raw.parquet")

    return True


if __name__ == "__main__":

    all_pids = os.listdir(f"{DIR}/pids")

    if not os.path.exists(f"{DIR}/patterns_raw"):
        os.makedirs(f"{DIR}/patterns_raw")

    for pid_file in all_pids:
        pid = pid_file.replace(".parquet", "")

        logging.debug(f"Querying raw pattern for PID: {pid}")
        pattern = query_cta_api(pid)
