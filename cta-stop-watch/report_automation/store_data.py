
import subprocess
from datetime import date

# Contants --------------------------------------------------------------------

# urls
s3_path = "gs://miurban-dj-public/cta-stop-watch"


# delete everything in the folder and then upload the new files

# Functions -------------------------------------------------------------------


def store_folder_data(
    s3_path: str, folder_path: str, s3_location: str, type: str = "cp"
) -> bool:
    """
    Replace all files in s3_location with files in data_path
    """

    command = f"gsutil {type} {folder_path}/* {s3_path}/{s3_location}"
    print(command)
    subprocess.run(command, shell=True)

    return True


def store_file(
    s3_path: str, file_path: str, s3_location: str, type: str = "cp"
) -> bool:
    """
    Replace all files in s3_location with files in data_path
    """

    command = f"gsutil {type} {file_path} {s3_path}/{s3_location}"
    subprocess.run(command, shell=True)

    return True


def store_all_data() -> None:
    """
    push data to s3 bucket, trips by pid, patterns, and timetables

    """
    today = str(date.today())

    store_folder_data(s3_path, "data/processed_by_pid", "processed_by_pid/", type="cp")
    store_folder_data(s3_path, "data/patterns/patterns_raw", "patterns_raw/", type="cp")
    store_folder_data(s3_path, "data/clean_timetables", "clean_timetables/", type="cp")

    store_file(
        s3_path,
        f"data/staging/timetables/feed_{today}.zip",
        f"historic_gtfs/feed_{today}.zip",
    )

    # rename current file in s3 bucket to old
    store_file(
        s3_path,
        f"{s3_path}/metrics/stop_metrics_df_latest.parquet",
        "metrics/stop_metrics_df_previous.parquet",
        type="mv",
    )

    # add latest file
    store_file(
        s3_path,
        "data/metrics/stop_metrics_df.parquet",
        "metrics/stop_metrics_df_latest.parquet",
        type="cp",
    )

    # rename current file in s3 bucket to old
    store_file(
        s3_path,
        f"{s3_path}/metrics/stop_metrics_df_latest.csv",
        "metrics/stop_metrics_df_previous.csv",
        type="mv",
    )

    # add latest file
    store_file(
        s3_path,
        "data/metrics/stop_metrics_df.csv",
        "metrics/stop_metrics_df_latest.csv",
        type="cp",
    )


# End -------------------------------------------------------------------------
