import pathlib
import os
from calculate_stop_time import calculate_patterns
from process_patterns import process_patterns
from qc_pipeline import qc_pipeline
from create_data_part import full_download
from extract_routes import extract_routes
from add_patterns_from_archive import main as patterns_historic
#from ..scrapers.process_historic_gtfs import main as process_historic_gtfs
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
        choices=["process_patterns", "stop_time", "qc", "full", "download_trips", 'download_patterns'],
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

    if args.pipeline_step == 'download_trips':
        full_download('2022-6-1', '2024-6-1')
        extract_routes()

    elif args.pipeline_step == "download_patterns":
        print("Downloading patterns")
        process_historic_gtfs()
    
    elif args.pipeline_step == "process_patterns":
        print(f"Processing {len(pids_pattern)} pattern(s)")
        process_patterns(pids_pattern)

        # process historic patterns
        patterns_historic()

    elif args.pipeline_step == "stop_time":
        print(f"Calculating stop time for {len(pids_calculate)} pattern(s)")

        calculate_start = time.time()
        calculate_patterns(pids_calculate)
        calculate_end = time.time()
        print(f"calculate_patterns total time taken: {(calculate_end - calculate_start)/60} minutes")

    elif args.pipeline_step == "qc":
        print(f"Running QC for for {len(pids_calculate)} pattern(s)")
        qc_pipeline(pids_calculate)

    elif args.pipeline_step == "full":
        # download and process bus data
        full_download('2022-6-1', '2024-6-1')
        extract_routes()

        # Process patterns
        print(f"Processing {len(pids_pattern)} pattern(s)")
        process_patterns(pids_pattern)

        # process historic patterns
        patterns_historic()

        # calculate stop time
        print(f"Calculating stop time for {len(pids_calculate)} pattern(s)")
        calculate_patterns(pids_calculate)

        # run qc
        print(f"Running QC for for {len(pids_calculate)} pattern(s)")
        qc_pipeline(pids_calculate)