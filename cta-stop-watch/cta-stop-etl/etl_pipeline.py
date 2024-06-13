import pathlib
import os
from calculate_stop_time import process_all_patterns, process_pattern
from process_patterns import convert_to_geometries
from qc_pipeline import qc_pipeline
import sys

if __name__ == "__main__":
    if len(sys.argv) > 4 or (len(sys.argv) == 2 and sys.argv[1] != "--qc"):
        print(
            "Usage: python -m cta-stop-watch.cta-stop-etl.etl_pipeline <[optional]--test> <[optional] pid> <[optional]--qc>"
        )
        sys.exit(1)

    elif len(sys.argv) == 3 and sys.argv[1] == "--test":
        pid = sys.argv[2]

        print(f"Test ETL Pipeline for 1000 trips on Pattern {pid}")

        print(f"Process Pattern {pid}")
        convert_to_geometries(pid)

        result = process_pattern(pid, 1000)

        DIR = pathlib.Path(__file__).parent / "out"

        if not os.path.exists(f"{DIR}/trips"):
            os.makedirs(f"{DIR}/trips")

        result.to_parquet(f"{DIR}/trips/test_trips_{pid}_full.parquet", index=False)

        print(f"Exported file to out/trips/test_trips_{pid}_full.parquet")
        print(f"Test ETL Pipeline for {pid} Complete")

        qc_pipeline(pid)

    elif len(sys.argv) <= 2:
        # pull in raw pattern data, process and write it out
        PATTERN_DIR = pathlib.Path(__file__).parent / "out/patterns_raw"

        all_patterns = os.listdir(PATTERN_DIR)

        print("Full ETL Pipeline for all Patterns and Trips")

        for pattern in all_patterns:
            # depending on naming convention, remove the file extension
            pattern = pattern.replace(".parquet", "")
            print(f"Processing Pattern: {pattern}")
            convert_to_geometries(pattern)

        # this script pulls all needed data from its local source and processes it
        # returns written df for each pattern
        print("Processing all trips")
        process_all_patterns()

        if len(sys.argv) == 2 and sys.argv[1] == "--qc":
            # qc script
            print("Running QC suite")
            qc_pipeline()

        print("ETL Pipeline Complete")
