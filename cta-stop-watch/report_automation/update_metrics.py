from stop_metrics import create_route_metrics_df, create_combined_metrics_stop_df
from metrics_utils import create_trips_df
from utils import metrics_logger
import polars as pl
import pandas as pd
import os
import pathlib
import duckdb

DIR = pathlib.Path(__file__).parent


def combine_recent_trips():
    # take whats in staging/trips combine it with processes_by_pid

    # get all the pids in the staging/trips
    pids = [
        name for name in os.listdir(f"{DIR}/data/staging/trips") if name != ".gitkeep"
    ]

    # stats before merging
    stats_command = """
    SELECT COUNT(*) as total_rows,
           COUNT(DISTINCT unique_trip_vehicle_day) as total_trips,  
           COUNT(DISTINCT pid) as total_pids,
           count(distinct CAST(bus_stop_time AS DATE)) as total_days,
           max(CAST(bus_stop_time AS DATE)) as max_date
    from read_parquet('data/processed_by_pid/*.parquet')"""

    stats_before = duckdb.execute(stats_command).df()

    metrics_logger.info(
        f"""Before merging, there were {stats_before['total_rows'].to_list()[0]:,} rows, 
        {stats_before['total_trips'].to_list()[0]:,} trips, 
        {stats_before['total_pids'].to_list()[0]:,} pids, 
        and {stats_before['total_days'].to_list()[0]:,} unique days. 
        The max date is {stats_before['max_date'].to_list()[0]}."""
    )

    for pid in pids:
        if os.path.exists(f"{DIR}/data/processed_by_pid/trips_{pid}_full.parquet"):
            # combine all the files together
            command = f"""COPY 
                            (SELECT * 
                            FROM read_parquet('data/staging/trips/{pid}/*')
                            UNION ALL
                            SELECT *
                            from read_parquet('data/processed_by_pid/trips_{pid}_full.parquet')
                            )  
                            TO 'data/processed_by_pid/trips_{pid}_full.parquet' (FORMAT 'parquet');
            """
            duckdb.execute(command)

        else:
            # copy over to storage folder
            # grab all days from the folder
            command = f"""COPY 
                            (SELECT * 
                            FROM read_parquet('data/staging/trips/{pid}/*'))  
                            TO 'data/processed_by_pid/trips_{pid}_full.parquet' (FORMAT 'parquet');
            """
            duckdb.execute(command)

    # stats after merging
    stats_after = duckdb.execute(stats_command).df()

    metrics_logger.info(
        f"""After merging, there were {stats_after['total_rows'].to_list()[0]:,} rows, 
        {stats_after['total_trips'].to_list()[0]:,} trips, 
        {stats_after['total_pids'].to_list()[0]:,} pids, 
        and {stats_after['total_days'].to_list()[0]:,} unique days. 
        The max date is {stats_after['max_date'].to_list()[0]}."""
    )


def update_metrics(rts: list | str):
    OUT_DIR = "data/metrics"

    # metric states before
    if os.path.exists(f"{OUT_DIR}/stop_metrics_df.parquet"):
        mertics_df = pd.read_parquet(f"{OUT_DIR}/stop_metrics_df.parquet")
        total_rows = mertics_df.shape[0]
        total_months = mertics_df[mertics_df["period"] == "month_abs"][
            "period_value"
        ].nunique()
        max_month = mertics_df[mertics_df["period"] == "month_abs"][
            "period_value"
        ].max()

        metrics_logger.info(
            f"""Before updating metrics, there were {total_rows:,} rows, 
            and {total_months:,} unique months. 
            The max month is {max_month}."""
        )
    else:
        metrics_logger.info("No metrics file found")

    if rts == "all":
        xwalk = pd.read_parquet("data/rt_to_pid.parquet")
        rts = xwalk["rt"].unique().tolist()

    # for each route

    all_routes_stops_actual = []
    all_routes_stops_schedule = []

    rts_count = 0

    for rt in rts:
        # prep schedule and actual
        metrics_logger.debug(f"Processing route {rt}")
        try:
            actual_df = create_trips_df(rt=rt, is_schedule=False)
            schedule_df = create_trips_df(rt=rt, is_schedule=True)
        except Exception as e:
            metrics_logger.info(f"issue with rt {rt}: {e}")
            continue

        # create the stop level data
        route_metrics_actual = create_route_metrics_df(actual_df, is_schedule=False)
        route_metrics_schedule = create_route_metrics_df(schedule_df, is_schedule=True)

        all_routes_stops_actual.append(route_metrics_actual)
        all_routes_stops_schedule.append(route_metrics_schedule)

        rts_count += 1

        if rts_count % 40 == 0:
            metrics_logger.info(f"{round((rts_count/len(rts)) * 100,3)} complete")

    # combine stop level at routes
    actual_full_stops = pl.concat(all_routes_stops_actual)
    schedule_full_stops = pl.concat(all_routes_stops_schedule)
    stop_metrics = create_combined_metrics_stop_df(
        actual_full_stops, schedule_full_stops
    )

    # export
    stop_metrics.write_parquet(f"{OUT_DIR}/stop_metrics_df.parquet")

    # metric states before
    mertics_df = pd.read_parquet(f"{OUT_DIR}/stop_metrics_df.parquet")
    total_rows = mertics_df.shape[0]
    total_months = mertics_df[mertics_df["period"] == "month_abs"][
        "period_value"
    ].nunique()
    max_month = mertics_df[mertics_df["period"] == "month_abs"]["period_value"].max()

    metrics_logger.info(
        f"""After updating metrics, there were {total_rows:,} rows, 
        and {total_months:,} unique months. 
        The max month is {max_month}."""
    )

    return True


if __name__ == "__main__":
    update_metrics("all")
