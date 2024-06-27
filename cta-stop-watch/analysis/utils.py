import polars as pl
import pandas as pd
from typing import Callable


def create_rt_pid_xwalk() -> bool:
    """
    Create a route to pattern id crosswalk called rt_to_pid.parquet
    """

    df = pl.scan_parquet("../cta-stop-etl/out/cta_bus_full_day_data_v2.parquet")
    xwalk = df.with_columns(pl.col("pid").cast(pl.Int32).cast(pl.String))

    xwalk.select(pl.col(["rt", "pid"])).unique(["rt", "pid"]).sink_parquet(
        "rt_to_pid.parquet"
    )

    return True


def create_trips_df(rts: list | str, is_scheduled: bool = False) -> pl.DataFrame:
    """
    Given rts to pids xwalk and a list of rts, create a df for all the trips for the rts
    """

    trips = []
    xwalk = pd.read_parquet("rt_to_pid.parquet")

    if rts == "all":
        rts = xwalk["rt"].unique().tolist()

    # just look at the rts for schedule
    if is_scheduled:
        iter = rts
        file_DIR = "../cta-stop-etl/out/clean_timetables/rt{rt}_timetable.parquet"
        error = "Do not have timetable for route {rt}. Skipping"
    else:
        data_set = (
            xwalk[xwalk["rt"].isin(rts)].groupby(["rt", "pid"]).count().reset_index()
        )
        pids = data_set.itertuples(index=False)
        iter = pids

        file_DIR = "../cta-stop-etl/out/trips/trips_{pid}_full.parquet"
        error = "Do not have pattern {pid} for route. Skipping"

    for obj in iter:
        if is_scheduled:
            rt = obj
            template_values = {"rt": rt}

        else:
            rt, pid = obj.rt, obj.pid
            template_values = {"pid": pid}

        try:

            df_trips = pl.read_parquet(file_DIR.format(**template_values))
        except FileNotFoundError:
            print(error.format(**template_values))
            continue

        if not is_scheduled:
            # for now
            df_trips = df_trips.filter(pl.col("typ") == "S")
            df_trips = df_trips.with_columns(
                pl.lit(pid).cast(pl.Int64).alias("pid"),
                pl.lit(rt).cast(pl.Int64).alias("rt"),
            )

        trips.append(df_trips)

    df_trips_all = pl.concat(trips)

    if is_scheduled:
        df_trips_all = df_trips_all.sort(["schd_trip_id", "bus_stop_time"])

        df_trips_all = df_trips_all.with_columns(
            total_stops=pl.col("stop_id").n_unique().over("pid"),
            trip_rn=pl.col("bus_stop_time").rank("ordinal").over("schd_trip_id"),
        )

        # create unique trip id. schd_trip_id is reused so count how many
        #  stops there are in a pattern and use that to determine when a
        #  trip ends and a new begins for trips with the same schd_trip_id
        df_trips_all = df_trips_all.with_columns(
            pl.struct("schd_trip_id", "total_stops", "trip_rn")
            .map_elements(
                lambda x: x["schd_trip_id"]
                + "-"
                + str(((x["trip_rn"] - 1) // x["total_stops"])),
                return_dtype=pl.String,
            )
            .alias("trip_id")
        )
        df_trips_all = df_trips_all.rename({"route_id": "rt"})

    else:
        df_trips_all = df_trips_all.rename(
            {
                "unique_trip_vehicle_day": "trip_id",
                "stpid": "stop_id",
                "seg_combined": "stop_sequence",
            }
        )

    return df_trips_all


def find_metric(
    is_schedule: bool,
    rts: list,
    metric: Callable,
    group_type: str = "stop_id",
) -> pl.DataFrame:
    """
    Calculate a metric for the given rts for other real bus locations of from the schedule.
    """

    trips_df = create_trips_df(rts=rts, is_scheduled=is_schedule)

    return metric(trips_df, group_type=group_type)
