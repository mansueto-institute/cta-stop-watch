from process_metrics import process_metrics
from process_trips import process_new_trips
from utils import create_config
import argparse


def parse_arguments():
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

    args = parser.parse_args()
    return args


if __name__ == "__main__":

    args = parse_arguments()
    if args.config == "config":
        create_config()
    elif args.pipeline_step[0] == "process":
        print("Processing new trips")
        process_new_trips()
    elif args.pipeline_step[0] == "metrics":
        if args.pipeline_step[1] == "local":
            process_metrics(local=True)
        elif args.pipeline_step[1] == "remote":
            process_metrics(local=False)
