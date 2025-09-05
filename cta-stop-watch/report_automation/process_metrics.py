from utils import metrics_logger, clear_staging
from store_data import store_all_data
from update_metrics import update_metrics, combine_recent_trips
from update_schedule import update_schedule

# Functions -------------------------------------------------------------------


def process_metrics(local: bool = True) -> None:
    """
    Implement workflow to process metrics data

    Args:
        local (bool): Flag indicating if the metrics are being run on a local machine

    Returns:
        None
    """

    # Combine recent trips
    metrics_logger.info("Combining trips")
    combine_recent_trips()
    metrics_logger.info("Done combining trips")
    clear_staging(folders=["staging/trips"])

    # Update schedule
    metrics_logger.info("Updating schedule")
    update_schedule()
    metrics_logger.info("Done updating schedule")
    clear_staging(folders=["staging/timetables/current_timetables"])

    # Update metrics
    metrics_logger.info("Updating metrics")
    update_metrics("all")
    metrics_logger.info("Done updating metrics")

    # Push all date to s3
    if not local:
        metrics_logger.info("Pushing data to s3")
        store_all_data()


# End -------------------------------------------------------------------------
