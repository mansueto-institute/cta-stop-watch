import requests

# from env import TRANSIT_LAND_API_KEY
import pandas as pd
from urllib.request import urlretrieve
import os
import pathlib
from dotenv import load_dotenv


# Load environment variables
load_dotenv()
TRANSIT_LAND_API_KEY = os.getenv("TRANSIT_LAND_API_KEY")

DIR = pathlib.Path(__file__).parent


def get_feeds():
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


def download_historic_feed(sha1: str):
    URL = "https://transit.land/api/v2/rest/feed_versions/"
    FULL_URL = URL + sha1 + "/download" + "?api_key=" + TRANSIT_LAND_API_KEY
    filename = f"inp/historic_gtfs/{sha1}.zip"

    if not os.path.exists(f"{DIR}/inp/historic_gtfs"):
        os.makedirs(f"{DIR}/inp/historic_gtfs")

    urlretrieve(FULL_URL, filename)

    return True


def main():
    historic_feeds = get_feeds()
    
    for feed in historic_feeds:
        print(f"Downloading {feed}")
        download_historic_feed(feed)

    return True


if __name__ == "__main__":
    main()
