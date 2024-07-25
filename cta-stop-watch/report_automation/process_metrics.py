from metrics_utils import create_rt_pid_xwalk
from utils import metrics_logger, clear_staging
from update_metrics import update_metrics, combine_recent_trips
from update_schedule import update_schedule


def process_metrics():

    # combine recent trips
    metrics_logger.info("Combining trips")
    combine_recent_trips()
    metrics_logger.info("Done combining trips")

    # update schedule
    metrics_logger.info("Updating schedule")
    update_schedule()
    metrics_logger.info("Done updating schedule")
    clear_staging(folder=["timetables/current_timetables"])

    # update metrics
    metrics_logger.info("Updating metrics")
    update_metrics("all")
    metrics_logger.info("Done updating metrics")
