"""
TransitLand API documentation: https://www.transit.land/documentation/rest-api/feeds
TransitLand CTA historic feeds: https://www.transit.land/feeds/f-dp3-cta
Downloading GTFS from TransitLand: https://www.transit.land/documentation/concepts/static-gtfs-feed-versions/#downloading-static-gtfs-feed-versions
"""

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
CTA_FEED = "f-dp3-cta"
DIR = pathlib.Path(__file__).parent


# Functions -------------------------------------------------------------------


def get_feeds_metadata() -> pd.DataFrame:
    """
    Query Transit Land Archive to get all available historic feeds for CTA's
    buses.

    Return:
        A DataFrame with the metadata of TransitLand available feeds
    """
    URL = "https://transit.land/api/v2/rest/"
    response = requests.get(
        URL + "feeds?api_key=" + TRANSIT_LAND_API_KEY + "&onestop_id=" + CTA_FEED
    )
    r = response.json()
    df = pd.DataFrame(r["feeds"][0]["feed_versions"])

    print("Writing metadata")
    df.to_parquet(f"{DIR}/inp/historic_gtfs/metadata.parquet")

    return df


def download_historic_feed(sha1: str, start_date: str = "", end_date: str = "") -> bool:
    """
    Query Transit Land Archive to dowload copy of every static feed stored in
    their archive.

    Args:
        sha1 (str): sha string that TransitLand uses to identify
                    the specific feed
        start_date (str): date string denoting the start of the period
                    where the schedule captured in the feed was valid
        end_date (str): date string denoting the end of the period
                    where the schedule capture in the feed was valid
    """
    URL = "https://transit.land/api/v2/rest/feed_versions/"
    FULL_URL = URL + sha1 + "/download" + "?api_key=" + TRANSIT_LAND_API_KEY

    print(f"Making request to {FULL_URL}")

    if start_date:
        filename = f"inp/historic_gtfs/{start_date}_{end_date}.zip"

    else:
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
    df_metadata = get_feeds_metadata()
    historic_feeds_shas = df_metadata[
        df_metadata["earliest_calendar_date"] >= "2022-03-01"
    ]["sha1"].tolist()

    for feed_sha in historic_feeds_shas:
        df_feed_metadata = df_metadata[df_metadata["sha1"] == feed_sha]
        start_date = df_feed_metadata["earliest_calendar_date"].values[0]
        end_date = df_metadata["latest_calendar_date"].values[0]

        print(
            f"Downloading feed {feed_sha}, with schedule from {start_date} to {end_date}"
        )
        download_historic_feed(feed_sha, start_date=start_date, end_date=end_date)

    return True


# Implementation --------------------------------------------------------------

if __name__ == "__main__":
    print("Retrieveing hitoric GTFS feeds from TransitLand")
    print(f"TransitLand API Key: {TRANSIT_LAND_API_KEY}")
    main()

# End -------------------------------------------------------------------------
