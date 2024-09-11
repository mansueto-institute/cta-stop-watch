import json
import logging
import math
import os

import functions_framework
import pandas as pd
import pendulum
import requests
from google.cloud import storage

# use for dev, but don't deploy to Lambda:
# from dotenv import load_dotenv
# load_dotenv()

logger = logging.getLogger()
logger.setLevel(logging.INFO)

BUCKET_PUBLIC = "miurban-test-bucket"


def scrape(routes_df, url):
    bus_routes = routes_df  # [routes_df.route_type == 3]
    response_json = json.loads("{}")
    for chunk in range(math.ceil(len(bus_routes) / 10)):
        chunk_routes = routes_df.iloc[
            [chunk * 10 + i for i in range(min(10, len(bus_routes) - (chunk * 10)))],
        ]
        route_query_string = chunk_routes.rt.str.cat(sep=",")
        logger.info(f"Requesting routes: {route_query_string}")
        try:
            chunk_response = json.loads(
                requests.get(url + f"&rt={route_query_string}" + "&format=json").text
            )
            response_json[f"chunk_{chunk}"] = chunk_response
        except requests.RequestException as e:
            logger.error("Error calling API")
            logger.error(e)
    logger.info("Data fetched")
    return response_json


@functions_framework.cloud_event
def lambda_handler(event):
    API_KEY = os.environ.get("CHN_GHOST_BUS_CTA_BUS_TRACKER_API_KEY")
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_PUBLIC)
    logger.info("Hitting API")
    api_url = (
        f"http://www.ctabustracker.com/bustime/api" f"/v2/getvehicles?key={API_KEY}"
    )

    routes_df = pd.read_csv("gs://miurban-test-bucket/routes.csv")
    logger.info("Loaded routes df")
    data = json.dumps(scrape(routes_df, api_url))
    logger.info("Saving data")
    t = pendulum.now("America/Chicago")
    key = f"bus_data/{t.to_date_string()}/{t.to_time_string()}.json"
    logger.info(f"Writing to {key}")
    blob = bucket.blob(key)
    blob.upload_from_string(data)


if __name__ == "__main__":
    lambda_handler(None)
