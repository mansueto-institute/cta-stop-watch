from stop_metrics import create_route_metrics_df, create_combined_metrics_stop_df
from metrics_utils import create_trips_df
from utils import metrics_logger, clear_staging
import pandas as pd
import os
import pathlib
import duckdb
from memory_profiler import profile

# Contants --------------------------------------------------------------------

# Paths
DIR = pathlib.Path(__file__).parent
OUT_DIR = DIR / "data" / "metrics"

# Functions -------------------------------------------------------------------


def combine_recent_trips() -> None:
    """
    take whats in staging/trips combine it with processes_by_pid
    """

    # get all the pids in the staging/trips
    # make sure folder id not empty
    pids = []
    for folder in os.listdir(f"{DIR}/data/staging/trips"):
        if folder != ".gitkeep":
            if os.listdir(f"{DIR}/data/staging/trips/{folder}/") != []:
                pids.append(folder)

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


@profile
def update_metrics(rts: list[str] | str = "all") -> bool:
    """
    combine new trips and then calculate new metrics

    Arguments:
        - rts: Can either be a list with the specific routes to be processed
        (represented as strings) or the string value "all" for processing
        all available routes.

    Returns: a boolean to confirm execution and writes data sets
    """

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
        # max month of actual data
        max_month_actual = mertics_df[
            (mertics_df["period"] == "month_abs")
            & (mertics_df["median_actual_time_till_next_bus"].notna())
        ]["period_value"].max()

        metrics_logger.info(
            f"""Before updating metrics, there were {total_rows:,} rows, 
            and {total_months:,} unique months. 
            The max month is {max_month}.
            The max actual month is {max_month_actual}
            """
        )
    else:
        metrics_logger.info("No metrics file found")

    if rts == "all":
        xwalk = pd.read_parquet("data/rt_to_pid.parquet")
        rts = xwalk["rt"].unique().tolist()

    rts_count = 0

    # create staging folders
    if not os.path.exists(OUT_DIR / "staging_actual"):
        os.mkdir(OUT_DIR / "staging_actual")
    if not os.path.exists(OUT_DIR / "staging_sched"):
        os.mkdir(OUT_DIR / "staging_sched")

    for rt in rts:
        # prep schedule and actual
        metrics_logger.debug(f"Processing route {rt}")
        try:
            actual_df = create_trips_df(rt=rt, is_schedule=False)

        except Exception as e:
            metrics_logger.info(f"issue with rt {rt}: {e}")
            continue

        route_metrics_actual = create_route_metrics_df(actual_df, is_schedule=False)

        # write out to file
        route_metrics_actual.write_parquet(
            f"{OUT_DIR}/staging_actual/route{rt}_metrics_actual.parquet"
        )

        del route_metrics_actual

        try:
            schedule_df = create_trips_df(rt=rt, is_schedule=True)
        except Exception as e:
            metrics_logger.info(f"issue with rt {rt}: {e}")
            continue

        route_metrics_schedule = create_route_metrics_df(schedule_df, is_schedule=True)
        # create the stop level data

        # write out to file
        route_metrics_schedule.write_parquet(
            f"{OUT_DIR}/staging_sched/route{rt}_metrics_schedule.parquet"
        )

        del route_metrics_schedule

        rts_count += 1
        if rts_count % 40 == 0:
            metrics_logger.info(f"{round((rts_count/len(rts)) * 100,3)} complete")

    # combine stop level at routes
    a_command = f""" select *
                    from read_parquet('{OUT_DIR}/staging_actual/*.parquet')
                    """
    actual_full_stops = duckdb.execute(a_command).pl()

    s_command = f""" select *
                    from read_parquet('{OUT_DIR}/staging_sched/*.parquet')
                    """
    actual_full_stops = duckdb.execute(a_command).pl()

    schedule_full_stops = duckdb.execute(s_command).pl()

    stop_metrics = create_combined_metrics_stop_df(
        actual_full_stops, schedule_full_stops
    )

    # export
    stop_metrics.write_parquet(f"{OUT_DIR}/stop_metrics_df.parquet")

    # metric states after
    mertics_df = pd.read_parquet(f"{OUT_DIR}/stop_metrics_df.parquet")
    total_rows = mertics_df.shape[0]
    total_months = mertics_df[mertics_df["period"] == "month_abs"][
        "period_value"
    ].nunique()
    max_month = mertics_df[mertics_df["period"] == "month_abs"]["period_value"].max()
    # max month of actual data
    max_month_actual = mertics_df[
        (mertics_df["period"] == "month_abs")
        & (mertics_df["median_actual_time_till_next_bus"].notna())
    ]["period_value"].max()

    metrics_logger.info(
        f"""After updating metrics, there were {total_rows:,} rows, 
        and {total_months:,} unique months. 
        The max month is {max_month}.
        The max actual month is {max_month_actual}"""
    )

    clear_staging(folders=["metrics/staging_actual", "metrics/staging_sched"])

    return True


# Implementation --------------------------------------------------------------

if __name__ == "__main__":
    update_metrics("all")

# End -------------------------------------------------------------------------
