from utils import metrics_logger, clear_staging
from store_data import store_all_data
from update_metrics import update_metrics, combine_recent_trips
from update_schedule import update_schedule

from datetime import date


def process_metrics(local: bool = True):

    # combine recent trips
    metrics_logger.info("Combining trips")
    combine_recent_trips()
    metrics_logger.info("Done combining trips")

    # update schedule
    metrics_logger.info("Updating schedule")
    update_schedule()
    metrics_logger.info("Done updating schedule")

    # update metrics
    metrics_logger.info("Updating metrics")
    update_metrics("all")
    metrics_logger.info("Done updating metrics")

    # push all date to s3
    if not local:
        metrics_logger.info("Pushing data to s3")
        store_all_data()

    # delete staging
    clear_staging(folders=["staging/timetables/current_timetables", "staging/trips"])
