# Libraries -------------------------------------------------------------------

import subprocess
from datetime import date

# Contants --------------------------------------------------------------------

# urls
BUCKET_PATH = "gs://miurban-dj-public/cta-stop-watch"


# delete everything in the folder and then upload the new files

# Functions -------------------------------------------------------------------


def store_folder_data(
    bucket_path: str, folder_path: str, bucket_location: str, type: str = "cp"
) -> bool:
    """
    Replace all files in bucket_location with files in data_path
    """

    command = f"gsutil {type} {folder_path}/* {bucket_path}/{bucket_location}"
    print(command)
    subprocess.run(command, shell=True)

    return True


def store_file(
    bucket_path: str, file_path: str, bucket_location: str, type: str = "cp"
) -> bool:
    """
    Replace all files in bucket_location with files in data_path
    """

    command = f"gsutil {type} {file_path} {bucket_path}/{bucket_location}"
    subprocess.run(command, shell=True)

    return True


def store_all_data() -> None:
    """
    push data to google storage bucket, trips by pid, patterns, and timetables

    """
    today = str(date.today())

    store_folder_data(
        BUCKET_PATH, "data/processed_by_pid", "processed_by_pid/", type="cp"
    )
    store_folder_data(
        BUCKET_PATH, "data/patterns/patterns_raw", "patterns_raw/", type="cp"
    )
    store_folder_data(
        BUCKET_PATH, "data/clean_timetables", "clean_timetables/", type="cp"
    )

    store_file(
        BUCKET_PATH,
        f"data/staging/timetables/feed_{today}.zip",
        f"historic_gtfs/feed_{today}.zip",
    )

    # rename current file in google storage bucket to old
    store_file(
        BUCKET_PATH,
        f"{BUCKET_PATH}/metrics/stop_metrics_df_latest.parquet",
        "metrics/stop_metrics_df_previous.parquet",
        type="mv",
    )

    # add latest file
    store_file(
        BUCKET_PATH,
        "data/metrics/stop_metrics_df.parquet",
        "metrics/stop_metrics_df_latest.parquet",
        type="cp",
    )

    # rename current file in google storage bucket to old
    store_file(
        BUCKET_PATH,
        f"{BUCKET_PATH}/metrics/stop_metrics_df_latest.csv",
        "metrics/stop_metrics_df_previous.csv",
        type="mv",
    )

    # add latest file
    store_file(
        BUCKET_PATH,
        "data/metrics/stop_metrics_df.csv",
        "metrics/stop_metrics_df_latest.csv",
        type="cp",
    )


# End -------------------------------------------------------------------------
