import json
import logging
import os
from typing import Optional

import pandas as pd
import pendulum
from google.cloud import storage

# use for dev, but don't deploy to Lambda:
# from dotenv import load_dotenv
# load_dotenv()

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# BUCKET_PRIVATE = os.getenv("PRIVATE", "miurban-dj-private")
BUCKET_PUBLIC = os.getenv("PUBLIC", "miurban-dj-public")

# logger = logging.getLogger()
logging.basicConfig(level=logging.INFO)


def combine_daily_files(date: str, save: Optional[str] = None):
    """Combine raw JSON files returned by API into daily CSVs.

    Args:
        date: Date string for which raw JSON files should be combined into CSVs. Format: YYYY-MM-DD.
    """
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_PUBLIC)

    logging.info(f"processing data from {BUCKET_PUBLIC}")
    # objects = bucket.objects.filter(Prefix=f"bus_data/{date}")
    objects = storage_client.list_blobs(
        BUCKET_PUBLIC, prefix=f"cta-stop-watch/daily-scrape/{date}"
    )

    logging.info(f"loaded objects to process for {date}")

    data_list = []
    errors_list = []
    counter = 0
    for obj in objects:
        counter += 1
        if counter % 20 == 0:
            logger.info(f"processing object # {counter}")
        obj_name = obj.name
        # https://stackoverflow.com/questions/31976273/open-s3-object-as-a-string-with-boto3
        obj_body = json.loads(obj.download_as_string())

        new_data = pd.DataFrame()
        new_errors = pd.DataFrame()

        # expect ~12 "chunks" per JSON
        for chunk in obj_body.keys():
            if "vehicle" in obj_body[chunk]["bustime-response"].keys():
                new_data = pd.concat(
                    [
                        new_data,
                        pd.DataFrame(obj_body[chunk]["bustime-response"]["vehicle"]),
                    ],
                    ignore_index=True,
                )
            if "error" in obj_body[chunk]["bustime-response"].keys():
                new_errors = pd.concat(
                    [
                        new_errors,
                        pd.DataFrame(obj_body[chunk]["bustime-response"]["error"]),
                    ],
                    ignore_index=True,
                )
            new_data["scrape_file"] = obj_name
            new_errors["scrape_file"] = obj_name

        data_list.append(new_data)
        errors_list.append(new_errors)

    data = pd.concat(data_list, ignore_index=True)
    errors = pd.concat(errors_list, ignore_index=True)

    logging.info(f"found {len(errors)} errors and {len(data)} data points for {date}")

    if len(errors) > 0:
        if save == "bucket":
            error_key = f"cta-stop-watch/bus_full_day_errors_v2/{date}.csv"
            logging.info(f"saving errors to {bucket}/{error_key}")
            blob = bucket.blob(error_key)
            blob.upload_from_string(errors.to_csv(index=False))
        if save == "local":
            local_filename = (
                f"ghost_buses_full_day_errors_from_{bucket.name}_{date}.csv"
            )
            logging.info(f"saving errors to {local_filename}")
            errors.to_csv(local_filename, index=False)
    else:
        logging.info(f"no errors found for {date}, not saving any error file")

    if len(data) > 0:
        # convert data time to actual datetime
        data["data_time"] = pd.to_datetime(data["tmstmp"], format="%Y%m%d %H:%M")

        data["data_hour"] = data.data_time.dt.hour
        data["data_date"] = data.data_time.dt.date
        if save == "bucket":
            data_key = f"cta-stop-watch/full_day_data/{date}.csv"
            logging.info(f"saving data to {bucket}/{data_key}")
            blob = bucket.blob(data_key)
            blob.upload_from_string(data.to_csv(index=False))
        if save == "local":
            local_filename = f"ghost_buses_full_day_data_from_{bucket.name}_{date}.csv"
            logging.info(f"saving errors to {local_filename}")
            data.to_csv(local_filename, index=False)
    else:
        logging.info(f"no data found for {date}, not saving any data file")

    return data, errors


def lambda_handler():
    date = pendulum.yesterday("America/Chicago").to_date_string()
    data, errors = combine_daily_files(date, save="bucket")

    # for i in range(1,12):
    #     date = (
    #         pendulum.yesterday("America/Chicago") - pendulum.duration(days=i)
    #     ).to_date_string()
    #     data, errors = combine_daily_files(date, save="bucket")


if __name__ == "__main__":
    lambda_handler()
