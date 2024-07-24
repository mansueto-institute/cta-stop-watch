from stop_metrics import create_route_metrics_df, create_combined_metrics_stop_df
from metrics_utils import create_trips_df
import polars as pl
import pandas as pd
import os
import pathlib
import duckdb

DIR = pathlib.Path(__file__).parent


def combine_recent_trips():
    # take whats in staging/trips combine it with processes_by_pid

    # get all the pids in the staging/trips
    pids = [name for name in os.listdir(f"{DIR}/data/staging/trips")]

    for pid in pids:

        if os.path.exists(f"{DIR}/data/processed_by_day/trips_{pid}_full.parquet"):
            # combine all the files together
            command = f"""COPY 
                            (SELECT * 
                            FROM read_parquet('data/staging/trips/{pid}/*')
                            UNION ALL
                            SELECT *
                            from read_parquet('data/processed_by_day/trips_{pid}_full.parquet')
                            )  
                            TO 'data/processed_by_day/trips_{pid}_full.parquet' (FORMAT 'parquet');
            """
            duckdb.execute(command)

        else:
            # copy over to storage folder
            # grab all days from the folder
            command = f"""COPY 
                            (SELECT * 
                            FROM read_parquet('data/staging/trips/{pid}/*'))  
                            TO 'data/processed_by_day/trips_{pid}_full.parquet' (FORMAT 'parquet');
            """
            duckdb.execute(command)


def update_metrics(rts, agg: bool = True):
    OUT_DIR = "data/metrics"

    if rts == "all":
        xwalk = pd.read_parquet("rt_to_pid.parquet")
        rts = xwalk["rt"].unique().tolist()

    # for each route

    all_routes_stops_actual = []
    all_routes_stops_schedule = []

    # all_routes_trips_actual = []
    # all_routes_trips_schedule = []

    for rt in rts:

        # prep schedule and actual
        print(f"Processing route {rt}")
        try:
            actual_df = create_trips_df(rt=rt, is_schedule=False)
            schedule_df = create_trips_df(rt=rt, is_schedule=True)
        except Exception as e:
            print(f"issue with rt {rt}: {e}")
            continue

        # create the stop level data
        route_metrics_actual = create_route_metrics_df(actual_df, is_schedule=False)
        route_metrics_schedule = create_route_metrics_df(schedule_df, is_schedule=True)

        all_routes_stops_actual.append(route_metrics_actual)
        all_routes_stops_schedule.append(route_metrics_schedule)

        # create the trip level data
        # trip_duration_actual = create_trips_metric_df(actual_df, is_schedule=False)
        # trip_duration_sched = create_trips_metric_df(schedule_df, is_schedule=True)

        # all_routes_trips_actual.append(trip_duration_actual)
        # all_routes_trips_schedule.append(trip_duration_sched)

    # combine trip level at routes
    # actual_full_trips = pl.concat(all_routes_trips_actual)
    # schedule_full_trips = pl.concat(all_routes_trips_schedule)

    # trip_metrics = create_combined_metrics_trip_df(
    #    actual_full_trips, schedule_full_trips
    # )

    # export
    # trip_metrics.write_parquet(f"{OUT_DIR}/trip_metrics_df_addon.parquet")

    # combine stop level at routes
    actual_full_stops = pl.concat(all_routes_stops_actual)
    schedule_full_stops = pl.concat(all_routes_stops_schedule)
    stop_metrics = create_combined_metrics_stop_df(
        actual_full_stops, schedule_full_stops
    )

    # export
    stop_metrics.write_parquet(f"{OUT_DIR}/stop_metrics_df.parquet")

    return True


if __name__ == "__main__":
    update_metrics("all")
