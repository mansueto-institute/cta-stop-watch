import subprocess

s3_path = "s3://cta-stop-watch-bucket-do/cta-stop-watch-files/"


def store_data(s3_path, data_path, s3_location):
    """
    Replace all files in s3_location with files in data_path
    """

    command = f"cd {data_path} | s3cmd put * {s3_path}/{s3_location}"
    subprocess.run(command, shell=True)

    return True


# store_data(s3_path, "data/processed_by_pid", "processed_by_pid/")
# store_data(s3_path, "data/patterns/patterns_raw", "patterns_raw/")
# store_data(s3_path, "data/clean_timetables", "clean_timetables/")

# store_data(s3_path, "data/staging/timetables", f"historic_gtfs/feed_{today}.zip")
