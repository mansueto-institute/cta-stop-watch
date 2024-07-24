from trip_processing import process_new_trips
from update_metrics import update_metrics

# from update_schedule import update_schedule
from utils import create_config
import argparse


def parse_arguments():
    parser = argparse.ArgumentParser(description="Run the StopWatch pipeline.")

    parser.add_argument(
        "-s",
        "--setup",
        type=str,
        help="Run setup scrips",
    )
    parser.add_argument(
        "-p",
        "--pipeline_step",
        type=str,
        choices=[
            "process",
            "metrics",
        ],
        help="Specify which part of the pipeline to run",
    )

    args = parser.parse_args()
    return args


if __name__ == "__main__":

    args = parse_arguments()
    print(args)
    if args.setup == "config":
        create_config()
    elif args.pipeline_step == "process":
        print("Processing new trips")
        process_new_trips()
    elif args.pipeline_step == "metrics":

        pass
        # update_schedule()
        # update_metrics("all")
