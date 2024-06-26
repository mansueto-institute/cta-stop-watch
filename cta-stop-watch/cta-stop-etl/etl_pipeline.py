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

logging.basicConfig(filename='pipeline.log', 
                    filemode="w",
                    encoding='utf-8', 
                    # level=logging.DEBUG
                    level=logging.INFO
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

    logging.info("STARTING PIPELINE EXECUTION -----------------------------------------\n")

    steps_executed = {}

    if args.pipeline_step == 'download_trips':
        step_start = time.time()
        full_download('2022-6-1', '2024-6-1')
        step_end = time.time()
        extract_routes()

    elif args.pipeline_step == "download_patterns":
        # print("Downloading patterns")
        logging.info("\t Downloading patterns\n")
        # process_historic_gtfs()
    
    elif args.pipeline_step == "process_patterns":
        # print(f"Processing {len(pids_pattern)} pattern(s)")
        logging.info(f"\tProcessing {len(pids_pattern)} pattern(s) \n")
        process_patterns(pids_pattern)

        # process historic patterns
        patterns_historic()

    elif args.pipeline_step == "stop_time":
        # print(f"Calculating stop time for {len(pids_calculate)} pattern(s)")
        logging.info(f"\tCalculating stop time for {len(pids_calculate)} pattern(s)\n")

        calculate_start = time.time()
        calculate_patterns(pids_calculate)
        calculate_end = time.time()
        # print(f"calculate_patterns total time taken: {(calculate_end - calculate_start)/60} minutes")
        logging.info(f"\tcalculate_patterns total time taken: {(calculate_end - calculate_start)/60} minutes")


    elif args.pipeline_step == "qc":
        # print(f"Running QC for for {len(pids_calculate)} pattern(s)")
        logging.info(f"\tRunning QC for for {len(pids_calculate)} pattern(s)\n")
        qc_pipeline(pids_calculate)

    elif args.pipeline_step == "full":
        # download and process bus data
        full_download('2022-6-1', '2024-6-1')
        extract_routes()

        # Process patterns
        # print(f"Processing {len(pids_pattern)} pattern(s)")
        logging.info(f"Processing {len(pids_pattern)} pattern(s)")
        process_patterns(pids_pattern)

        # process historic patterns
        patterns_historic()

        # calculate stop time
        # print(f"Calculating stop time for {len(pids_calculate)} pattern(s)")
        logging.info(f"Calculating stop time for {len(pids_calculate)} pattern(s)\n")
        calculate_patterns(pids_calculate)

        # run qc
        # print(f"Running QC for for {len(pids_calculate)} pattern(s)")
        logging.info(f"Running QC for for {len(pids_calculate)} pattern(s)\n")
        qc_pipeline(pids_calculate)


    # Exit pipeline and show time of execution
    end_tmstmp = time.time()
    end_string = time.asctime(time.localtime())

    logging.info("EXITING PIPELINE EXECUTION ------------------------------------------\n")
    logging.info("Runing time results -------------------------------------------------\n")
    logging.info(f"Finished running pipeline at {end_string}")
    total_running_time = end_tmstmp - start_tmstmp
    logging.info(f"Total running time {total_running_time}")