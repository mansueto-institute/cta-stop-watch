from stop_metrics import create_route_metrics_df, create_combined_metrics_stop_df
from trip_metrics import create_trips_metric_df, create_combined_metrics_trip_df
from utils import create_trips_df
import polars as pl
import pandas as pd


def create_metrics(rts, agg: bool = True):
    OUT_DIR = "out/"

    if rts == "all":
        xwalk = pd.read_parquet("rt_to_pid.parquet")
        rts = xwalk["rt"].unique().tolist()

    # for each route

    all_routes_stops_actual = []
    all_routes_stops_schedule = []

    all_routes_trips_actual = []
    all_routes_trips_schedule = []

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
        trip_duration_actual = create_trips_metric_df(actual_df, is_schedule=False)
        trip_duration_sched = create_trips_metric_df(schedule_df, is_schedule=True)

        all_routes_trips_actual.append(trip_duration_actual)
        all_routes_trips_schedule.append(trip_duration_sched)

    # combine trip level at routes
    actual_full_trips = pl.concat(all_routes_trips_actual)
    schedule_full_trips = pl.concat(all_routes_trips_schedule)

    # trip_metrics = create_combined_metrics_trip_df(
    #     actual_full_trips, schedule_full_trips
    # )

    # export

    actual_full_trips.write_parquet(f"{OUT_DIR}/actual_trip_metrics_df.parquet")
    schedule_full_trips.write_parquet(f"{OUT_DIR}/schedule_trip_metrics_df.parquet")

    # combine stop level at routes
    actual_full_stops = pl.concat(all_routes_stops_actual)
    schedule_full_stops = pl.concat(all_routes_stops_schedule)
    # stop_metrics = create_combined_metrics_stop_df(
    #     actual_full_stops, schedule_full_stops
    # )

    # export
    actual_full_stops.write_parquet(f"{OUT_DIR}/actual_stop_metrics_df.parquet")
    schedule_full_stops.write_parquet(f"{OUT_DIR}/schedule_stop_metrics_df.parquet")

    return True


if __name__ == "__main__":
    create_metrics("all")
