import pathlib
import os
from calculate_stop_time import process_all_patterns, process_pattern
from process_patterns import convert_to_geometries
from qc_pipeline import qc_pipeline
import sys
import time
import re

if __name__ == "__main__":

    DIR = pathlib.Path(__file__).parent / "out"

    if len(sys.argv) > 4 or (
        len(sys.argv) == 2 and sys.argv[1] not in ["--qc", "--full_pipeline"]
    ):
        print(
            "Usage: python -m etl_pipeline <[optional] --test> <[optional] pid> <[--process_pattern / --stop_time / --qc /--full_pipeline>"
        )
        sys.exit(1)

    if sys.argv[1] == "--test":
        if not sys.argv[2].isnumeric():
            print("Please provide a valid pattern id")
            print(
                "Usage: python -m etl_pipeline <[optional] --test> <[optional] pid> <[--process_pattern / --stop_time / --qc /--full_pipeline>"
            )
            sys.exit(1)
        if sys.argv[3] == "--process_pattern":
            print(f"Processing Pattern {sys.argv[2]}")
            convert_to_geometries(sys.argv[2])
        elif sys.argv[3] == "--stop_time":
            if not os.path.exists(f"{DIR}/trips"):
                os.makedirs(f"{DIR}/trips")
            print(f"Processing stop time for all trips of pattern {sys.argv[2]}")
            start = time.time()
            process_pattern(sys.argv[2])
            end = time.time()
            print(f"Time taken: {(end - start) / 60} minutes")
        elif sys.argv[3] == "--qc":
            print(f"Runing QC script for {sys.argv[2]}")
            qc_pipeline(sys.argv[2])
        elif sys.argv[3] == "--full_pipeline":
            print(f"Processing Pattern {sys.argv[2]}")
            try:
                convert_to_geometries(sys.argv[2])
            except:
                print(f"Do not have pattern {sys.argv[2]} available currently")
            if not os.path.exists(f"{DIR}/trips"):
                os.makedirs(f"{DIR}/trips")
            print(f"Processing Stop Time for all trips of pattern {sys.argv[2]}")
            start = time.time()
            process_pattern(sys.argv[2])
            end = time.time()
            print(f"Time taken: {(end - start) / 60} minutes")
            # print(f"Runing QC script for {sys.argv[2]}")
            # qc_pipeline(sys.argv[2])
    elif sys.argv[1] == "--full_pipeline":

        DIR = pathlib.Path(__file__).parent / "out"

        if not os.path.exists(f"{DIR}/trips"):
            os.makedirs(f"{DIR}/trips")

        print("Processing Patterns")
        PID_DIR = f"{DIR}/pids"
        pids = []
        for pid_file in os.listdir(PID_DIR):
            numbers = re.findall(r"\d+", pid_file)
            pid = numbers[0]
            pids.append(pid)

        pids = set(pids)
        for pid in pids:
            try:
                convert_to_geometries(pid)
            except:
                print(f"Do not have pattern {pid} available currently")
                continue

            print(f"Processing stop time for all trips of pattern {pid}")
            start = time.time()
            result = process_pattern(pid)
            end = time.time()

            result.to_parquet(f"{DIR}/trips/trips_{pid}_full.parquet", index=False)

            print(f"Time taken for stop times for {pid}: {(end - start) / 60} minutes")
