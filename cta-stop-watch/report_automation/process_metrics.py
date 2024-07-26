from utils import metrics_logger, clear_staging
from store_data import store_folder_data, store_file
from update_metrics import update_metrics, combine_recent_trips
from update_schedule import update_schedule

from datetime import date


def process_metrics():

    # combine recent trips
    metrics_logger.info("Combining trips")
    combine_recent_trips()
    metrics_logger.info("Done combining trips")

    # update schedule
    metrics_logger.info("Updating schedule")
    update_schedule()
    metrics_logger.info("Done updating schedule")
    clear_staging(folders=["timetables/current_timetables"])

    # update metrics
    metrics_logger.info("Updating metrics")
    update_metrics("all")
    metrics_logger.info("Done updating metrics")

    # push all date to s3
    metrics_logger.info("Pushing data to s3")
    s3_path = "s3://cta-stop-watch-bucket-do/cta-stop-watch-files/"
    today = str(date.today())

    store_folder_data(
        s3_path, "data/processed_by_pid", "processed_by_pid/", delete=True
    )
    store_folder_data(
        s3_path, "data/patterns/patterns_raw", "patterns_raw/", delete=True
    )
    store_folder_data(
        s3_path, "data/clean_timetables", "clean_timetables/", delete=True
    )

    store_file(
        s3_path,
        "data/staging/timetables",
        "feed_{today}.zip",
        f"historic_gtfs/feed_{today}.zip",
        delete=False,
    )
