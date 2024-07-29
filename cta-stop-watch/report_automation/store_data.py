import subprocess
from datetime import date

s3_path = "s3://cta-stop-watch-bucket-do/cta-stop-watch-files"

# delete everything in the folder and then upload the new files


def store_folder_data(s3_path, folder_path, s3_location, delete=True):
    """
    Replace all files in s3_location with files in data_path
    """

    if delete:
        command = f"s3cmd del {s3_path}/{s3_location} --recursive"
        subprocess.run(command, shell=True)

    command = f"s3cmd put {folder_path}/* {s3_path}/{s3_location} --recursive"
    # print(command)
    subprocess.run(command, shell=True)

    return True


def store_file(s3_path, file_path, s3_location, delete=True):
    """
    Replace all files in s3_location with files in data_path
    """

    if delete:
        command = f"s3cmd del {s3_path}/{s3_location}"
        subprocess.run(command, shell=True)

    command = f"s3cmd put {file_path} {s3_path}/{s3_location}"
    subprocess.run(command, shell=True)

    return True


def store_all_data():
    """
    push data to s3 bucket, trips by pid, patterns, and timetables

    """
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
        f"data/staging/timetables/feed_{today}.zip",
        f"historic_gtfs/feed_{today}.zip",
        delete=False,
    )
