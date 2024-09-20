import subprocess
from datetime import date

# Contants --------------------------------------------------------------------

# urls
s3_path = "gs://miurban-dj-private/cta-stop-watch"


# delete everything in the folder and then upload the new files

# Functions -------------------------------------------------------------------


def store_folder_data(
    s3_path: str, folder_path: str, s3_location: str, delete: bool = True
) -> bool:
    """
    Replace all files in s3_location with files in data_path
    """

    if delete:
        command = f"gsutil -m rm -r {s3_path}/{s3_location}"
        subprocess.run(command, shell=True)

    command = f"gsutil mv -m {folder_path}/* {s3_path}/{s3_location}"
    # print(command)
    subprocess.run(command, shell=True)

    return True


def store_file(
    s3_path: str, file_path: str, s3_location: str, delete: bool = True
) -> bool:
    """
    Replace all files in s3_location with files in data_path
    """

    if delete:
        command = f"gsutil rm {s3_path}/{s3_location}"
        subprocess.run(command, shell=True)

    command = f"gsutil mv {file_path} {s3_path}/{s3_location}"
    subprocess.run(command, shell=True)

    return True


def store_all_data() -> None:
    """
    push data to s3 bucket, trips by pid, patterns, and timetables

    """
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
        f"data/staging/timetables/feed_{today}.zip",
        f"historic_gtfs/feed_{today}.zip",
        delete=False,
    )

    store_file(
        s3_path,
        "data/metrics/stop_metrics_df.parquet",
        "metrics/stop_metrics_df.parquet",
        delete=True,
    )

    store_file(
        s3_path,
        "data/metrics/stop_metrics_df.csv",
        "metrics/stop_metrics_df.csv",
        delete=True,
    )


# End -------------------------------------------------------------------------
