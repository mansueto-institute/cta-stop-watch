import pathlib
import os
from calculate_stop_time import calculate_patterns
from process_patterns import process_patterns
from qc_pipeline import qc_pipeline
from create_data_part import full_download
from extract_routes import extract_routes
from add_patterns_from_archive import main as patterns_historic

# from ..scrapers.process_historic_gtfs import main as process_historic_gtfs
import time
import re
import argparse
import logging

# Logger ----------------------------------------------------------------------

logger = logging.getLogger(__name__)

logging.basicConfig(
    filename="pipeline.log",
    filemode="w",
    encoding="utf-8",
    level=logging.DEBUG,
    # level=logging.INFO
)

start_tmstmp = time.time()
start_string = time.asctime(time.localtime())
logging.info(f"CTA BUSES ETL PIPELINE STARTED AT: {start_string}")


# Functions -------------------------------------------------------------------


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
        choices=[
            "process_patterns",
            "stop_time",
            "qc",
            "full",
            "download_trips",
            "download_patterns",
        ],
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
            # Skip auxiliary files in folder, only process parquets
            if not pid_file.endswith(".parquet"):
                continue
            numbers = re.findall(r"\d+", pid_file)
            pid = numbers[0]
            pids.append(pid)
    elif type == "processed_patterns":
        pattern_current_DIR = f"{DIR}/patterns_current"
        pattern_historic_DIR = f"{DIR}/patterns_historic"

        pids_c = []
        for pid_file in os.listdir(pattern_current_DIR):

            if not pid_file.endswith(".parquet"):
                continue

            numbers = re.findall(r"\d+", pid_file)
            pid = numbers[0]
            pids_c.append(pid)

        pids_h = []
        for pid_file in os.listdir(pattern_historic_DIR):
            if not pid_file.endswith(".parquet"):
                continue

            numbers = re.findall(r"\d+", pid_file)
            pid = numbers[0]
            pids_h.append(pid)

        pids = set(pids_c + pids_h)

    return pids


def print_timing_at_exit() -> None:
    """
    Produces a footer for the pipeline.log file produced with
    logging. This is a common format to be printed at any run of the format.
    """
    # Exit pipeline and show time of execution
    end_tmstmp = time.time()
    end_string = time.asctime(time.localtime())

    logging.info("EXITING PIPELINE EXECUTION" + f"{'-'*43}" + "\n")
    logging.info("Time results" + f"{'-'*57}")
    logging.info(f"Finished running pipeline at {end_string}")
    total_running_time = end_tmstmp - start_tmstmp
    formatted_time = time.strftime("%H:%M:%S", time.gmtime(total_running_time))
    logging.info(f"Total running time {formatted_time}")


def execute_download_trips(print=True):
    logging.info("\tDownloading trips\n")
    tmstmp1 = time.time()
    full_download("2022-6-1", "2024-6-1")
    tmstmp2 = time.time()
    logging.info("\tDownloading routes\n")
    extract_routes()
    tmstmp3 = time.time()

    diff1 = tmstmp2 - tmstmp1
    diff2 = tmstmp3 - tmstmp2
    execution_time1 = (
        f"Time downloading trips: {time.strftime('%H:%M:%S', time.gmtime(diff1))}"
    )
    execution_time2 = (
        f"Time downloading routes: {time.strftime('%H:%M:%S', time.gmtime(diff2))}"
    )

    if print:
        print_timing_at_exit()
        logging.info(execution_time1)
        logging.info(execution_time2)
        logging.info(f"{'-'*69}")
        return None
    return execution_time1, execution_time2


def execute_download_patterns(print=True):
    # print("Downloading patterns")
    logging.info("\tDownloading patterns\n")
    tmstmp1 = time.time()
    # process_historic_gtfs()

    diff1 = time.time() - tmstmp1
    execution_time = (
        f"Time downloading patterns: {time.strftime('%H:%M:%S', time.gmtime(diff1))}"
    )

    if print:
        print_timing_at_exit()
        logging.info(execution_time)
        logging.info(f"{'-'*69}")
        return None
    return execution_time


def execute_process_patterns(pids_pattern, print=True):
    # print(f"Processing {len(pids_pattern)} pattern(s)")
    logging.info(f"\tProcessing {len(pids_pattern)} pattern(s) \n")
    tmstmp1 = time.time()
    process_patterns(pids_pattern)
    tmstmp2 = time.time()
    # process historic patterns
    patterns_historic()
    tmstmp3 = time.time()

    diff1 = tmstmp2 - tmstmp1
    diff2 = tmstmp3 - tmstmp2
    execution_time1 = f"Time processing CTA API patterns: {time.strftime('%H:%M:%S', time.gmtime(diff1))}"
    execution_time2 = f"Time processing historic patterns: {time.strftime('%H:%M:%S', time.gmtime(diff2))}"

    if print:
        print_timing_at_exit()
        logging.info(execution_time1)
        logging.info(execution_time2)
        logging.info(f"{'-'*69}")
        return None
    return execution_time1, execution_time2


def execute_stop_time(pids_calculate, print=True):
    # print(f"Calculating stop time for {len(pids_calculate)} pattern(s)")
    logging.info(f"\tCalculating stop time for {len(pids_calculate)} pattern(s)\n")

    tmstmp1 = time.time()
    calculate_patterns(pids_calculate)

    diff1 = time.time() - tmstmp1
    execution_time = (
        f"Time computing stop time: {time.strftime('%H:%M:%S', time.gmtime(diff1))}"
    )

    if print:
        print_timing_at_exit()
        logging.info(execution_time)
        logging.info(f"{'-'*69}")
        return None
    return execution_time


def execute_qc(pids_calculate, print=True):
    # print(f"Running QC for for {len(pids_calculate)} pattern(s)")
    logging.info(f"\tRunning QC for for {len(pids_calculate)} pattern(s)\n")
    tmstmp1 = time.time()
    qc_pipeline(pids_calculate)

    diff1 = time.time() - tmstmp1
    execution_time = f"Time performing quality chekcs (QC): {time.strftime('%H:%M:%S', time.gmtime(diff1))}"

    if print:
        print_timing_at_exit()
        logging.info(execution_time)
        logging.info(f"{'-'*69}")
        return None
    return execution_time


# Implementation --------------------------------------------------------------

if __name__ == "__main__":

    DIR = pathlib.Path(__file__).parent / "out"
    args = parse_arguments()
    logging.info(f"\tPipeline Step  : {args.pipeline_step}")

    if args.test_pid is not None:
        logging.info(f"\tPIDs to Process: {args.test_pid}\n")
        pids_pattern = [args.test_pid]
        pids_calculate = [args.test_pid]
    else:
        logging.info("\tPIDs to Process: ALL\n")
        pids_pattern = all_pids(DIR, "trip_data")
        pids_calculate = all_pids(DIR, "processed_patterns")

    logging.info("STARTING PIPELINE EXECUTION" + f"{'-'*42}" + "\n")

    if args.pipeline_step == "download_trips":
        execute_download_trips()
    elif args.pipeline_step == "download_patterns":
        execute_download_patterns()
    elif args.pipeline_step == "process_patterns":
        execute_process_patterns(pids_pattern)
    elif args.pipeline_step == "stop_time":
        execute_stop_time(pids_calculate)
    elif args.pipeline_step == "qc":
        execute_qc(pids_calculate)
    elif args.pipeline_step == "full":
        time_dtrip, time_droute = execute_download_trips(print=False)
        time_dpattern = execute_download_patterns(print=False)
        time_pcta, time_phistoric = execute_process_patterns(pids_pattern, print=False)
        time_stoptime = execute_stop_time(pids_calculate, print=False)
        time_qc = execute_qc(pids_calculate, print=False)

        # Print execution times of full pipeline
        print_timing_at_exit()
        logging.info(time_dtrip)
        logging.info(time_droute)
        logging.info(time_dpattern)
        logging.info(time_pcta)
        logging.info(time_phistoric)
        logging.info(time_stoptime)
        logging.info(time_qc)
        logging.info(f"{'-'*69}")
