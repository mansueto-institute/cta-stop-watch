# Imports ---------------------------------------------------------------------

from process_metrics import process_metrics
from process_trips import process_new_trips
from utils import create_config, process_logger, metrics_logger
import argparse

# Functions -------------------------------------------------------------------


def parse_arguments() -> argparse.Namespace:
    """
    Parse terminal arguments into python objects needed for pipeline workflow.
    """

    parser = argparse.ArgumentParser(description="Run the StopWatch pipeline.")

    parser.add_argument(
        "-c",
        "--config",
        type=str,
        nargs="?",
        const="config",
        help="Create a config file",
    )

    parser.add_argument(
        "-p",
        "--pipeline_step",
        type=str,
        nargs="+",
        choices=["process", "metrics", "local", "remote"],
        help="Specify which part of the pipeline to run",
    )

    pipelines_args = parser.parse_args()
    return pipelines_args


def run_main():
    usage_specs = """
        Incorrect program specification. 

        Correct usage to set up piepeline config: 
        -----------------------------------------
        $ python -m main -c

        Correct usage to run pipeline:
        ------------------------------
        $ python -m main -p [pipeline_step] [machine]
            Valid pipeline_step values: 'process' or 'metrics'
            Valid machine values: 'local' (default), or 'remote'

        """

    args = parse_arguments()

    if args.pipeline_step is None and args.config is None:
        print(usage_specs)
        return

    if args.config == "config":
        print("Creating config")
        create_config()
    elif args.pipeline_step[0] == "process":
        print("Processing new trips...")
        process_logger.info(
            f"\n{'-'*80}\n\n STARTING PROCESSING PIPELINE STEP FOR STOPWATCH"
        )
        process_new_trips()
    elif args.pipeline_step[0] == "metrics":
        print("Updating metrics...")
        process_logger.info(
            f"\n{'-'*80}\n\n STARTING METRICS PIPELINE STEP FOR STOPWATCH"
        )
        if args.pipeline_step[1] == "local":
            process_metrics(local=True)
        elif args.pipeline_step[1] == "remote":
            try:
                process_metrics(local=False)
            except Exception as e:
                metrics_logger.error(f"Error: {e}")


# Implementation --------------------------------------------------------------

if __name__ == "__main__":
    run_main()

# End -------------------------------------------------------------------------
