# Libraries -------------------------------------------------------------------
import requests

# from env import TRANSIT_LAND_API_KEY
import pandas as pd
from urllib.request import urlretrieve
import os
import pathlib
from dotenv import load_dotenv


# Constants -------------------------------------------------------------------

# Load environment variables
load_dotenv()
TRANSIT_LAND_API_KEY = os.getenv("TRANSIT_LAND_API_KEY")
DIR = pathlib.Path(__file__).parent


# Functions -------------------------------------------------------------------


def get_feeds() -> pd.DataFrame:
    """
    Query Transit Land Archive to get all available historic feeds for CTA's
    buses.
    """
    URL = "https://transit.land/api/v2/rest/"
    response = requests.get(
        URL + "feeds?api_key=" + TRANSIT_LAND_API_KEY + "&onestop_id=f-dp3-cta"
    )
    r = response.json()
    df = pd.DataFrame(r["feeds"][0]["feed_versions"])

    print("Writing metadata")
    df.to_parquet(f"{DIR}/inp/historic_gtfs/metadata.parquet")

    historic_feeds = df[df["earliest_calendar_date"] >= "2022-03-01"]["sha1"].tolist()
    return historic_feeds


def download_historic_feed(sha1: str) -> bool:
    """
    Query Transit Land Archive to dowload copy of every static feed stored in
    their archive.

    Args:
        sha1 (str): sha string that TransitLand uses to identify
                    the specific feed
    """
    URL = "https://transit.land/api/v2/rest/feed_versions/"
    FULL_URL = URL + sha1 + "/download" + "?api_key=" + TRANSIT_LAND_API_KEY
    filename = f"inp/historic_gtfs/{sha1}.zip"

    if not os.path.exists(f"{DIR}/inp/historic_gtfs"):
        os.makedirs(f"{DIR}/inp/historic_gtfs")

    urlretrieve(FULL_URL, filename)

    return True


def main():
    """
    Execute pipeline for downloading all available GTFS feeds for
    CTA Buses stored in TransitLand.
    """
    historic_feeds = get_feeds()

    for feed in historic_feeds:
        print(f"Downloading {feed}")
        download_historic_feed(feed)

    return True


# Implementation --------------------------------------------------------------

if __name__ == "__main__":
    print("Retrieveing hitoric GTFS feeds from TransitLand")
    main()

# End -------------------------------------------------------------------------
