from metrics_utils import create_rt_pid_xwalk
from utils import metric_logger, clear_staging
from update_metrics import update_metrics, combine_recent_trips
from update_schedule import update_schedule


def process_metrics():

    # combine recent trips
    combine_recent_trips()

    # create new xwalk for metrics
    create_rt_pid_xwalk()

    # update schedule
    update_schedule()
    clear_staging(["timetables/current_timetables"])

    # update metrics
    update_metrics("all")
