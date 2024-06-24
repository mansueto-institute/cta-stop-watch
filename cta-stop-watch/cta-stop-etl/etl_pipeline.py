import pathlib
import os
from calculate_stop_time import calculate_patterns
from process_patterns import process_patterns
from qc_pipeline import qc_pipeline
import sys
import time
import re
import argparse


def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Run the full or partial ETL pipeline."
    )
    parser.add_argument(
        "-t",
        "--test_pid",
        type=int,
        help="Allows for a run of the pipeline with a specific pattern id",
    )
    parser.add_argument(
        "-p",
        "--pipeline_step",
        type=str,
        required=True,
        choices=["process_pattern", "stop_time", "qc", "full", "download_trips"],
        help="Specify which part of the pipeline to run",
    )

    args = parser.parse_args()
    return args


def all_pids(DIR, type):
    """
    Find list of all pids currently available.
    """

    if type == "trip_data":
        PID_DIR = f"{DIR}/pids"
        pids = []
        for pid_file in os.listdir(PID_DIR):
            numbers = re.findall(r"\d+", pid_file)
            pid = numbers[0]
            pids.append(pid)
    elif type == "processed_patterns":
        pattern_current_DIR = f"{DIR}/patterns_current"
        pattern_historic_DIR = f"{DIR}/patterns_historic"

        pids_c = []
        for pid_file in os.listdir(pattern_current_DIR):
            numbers = re.findall(r"\d+", pid_file)
            pid = numbers[0]
            pids_c.append(pid)

        pids_h = []
        for pid_file in os.listdir(pattern_historic_DIR):
            numbers = re.findall(r"\d+", pid_file)
            pid = numbers[0]
            pids_h.append(pid)

        pids = set(pids_c + pids_h)

    return pids


if __name__ == "__main__":

    args = parse_arguments()
    DIR = pathlib.Path(__file__).parent / "out"

    if args.test_pid is not None:
        pids_pattern = [args.test_pid]
        pids_calculate = [args.test_pid]
    else:
        pids_pattern = all_pids(DIR, "trip_data")
        pids_calculate = all_pids(DIR, "processed_patterns")

    if args.pipeline_step == "process_pattern":
        print(f"Processing {len(pids_pattern)} pattern(s)")
        process_patterns(pids_pattern)

    elif args.pipeline_step == "stop_time":
        print(f"Calculating stop time for {len(pids_calculate)} pattern(s)")

        calculate_start = time.time()
        calculate_patterns(pids_calculate)
        calculate_end = time.time()
        print(f"calculate_patterns time taken: {(calculate_end - calculate_start)} minutes")

    elif args.pipeline_step == "qc":
        print(f"Running QC for for {len(pids_calculate)} pattern(s)")
        qc_pipeline(pids_calculate)

    elif args.pipeline_step == "full":
        # download and process bus data
        # TODO

        # Process patterns
        print(f"Processing {len(pids_pattern)} pattern(s)")
        process_patterns(pids_pattern)

        # calculate stop time
        print(f"Calculating stop time for {len(pids_calculate)} pattern(s)")
        calculate_patterns(pids_calculate)

        # run qc
        print(f"Running QC for for {len(pids_calculate)} pattern(s)")
        qc_pipeline(pids_calculate)

"""
    # TODO use arg parser
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
            except FileNotFoundError:
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
            except FileNotFoundError:
                print(f"Do not have pattern {pid} available currently")
                continue

            print(f"Processing stop time for all trips of pattern {pid}")
            start = time.time()
            result = process_pattern(pid)
            end = time.time()

            result.to_parquet(f"{DIR}/trips/trips_{pid}_full.parquet", index=False)

            print(f"Time taken for stop times for {pid}: {(end - start) / 60} minutes")
"""
