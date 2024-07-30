import polars as pl
import pandas as pd
import pathlib
from utils import metrics_logger

DIR = pathlib.Path(__file__).parent / "data"


def create_trips_df(rt: str, is_schedule: bool = False) -> pl.DataFrame:
    """
    Given rts to pids xwalk and a list of rts, create a df for all the trips for the rts
    """

    trips = []
    xwalk = pd.read_parquet(f"{DIR}/rt_to_pid.parquet")

    # just look at the rts for schedule
    if is_schedule:
        iter = [rt]
        file_DIR = f"{DIR}/clean_timetables/rt{rt}_timetable.parquet"
        error = "Do not have timetable for route {rt}. Skipping"
    else:
        data_set = xwalk[xwalk["rt"] == rt].groupby(["rt", "pid"]).count().reset_index()
        pids = data_set.itertuples(index=False)
        iter = pids

        file_DIR = str(DIR) + "/processed_by_pid/trips_{pid}_full.parquet"
        error = "Do not have pattern {pid} for route. Skipping"

    for obj in iter:
        if is_schedule:
            rt = obj
            template_values = {"rt": rt}

        else:
            rt, pid = obj.rt, obj.pid
            template_values = {"pid": pid}

        try:
            df_trips = pl.read_parquet(file_DIR.format(**template_values))
        except FileNotFoundError:
            metrics_logger.debug(error.format(**template_values))
            continue

        if "stop_dist" in df_trips.columns:
            df_trips = df_trips.drop("stop_dist")

        df_trips = df_trips.with_columns(pl.exclude("bus_stop_time").cast(pl.String))

        df_trips = df_trips.with_columns(
            pl.col("pid").cast(pl.Float64).cast(pl.Int32).cast(pl.String).alias("pid")
        )

        # convert bus_stop_time to nanoseconds
        df_trips = df_trips.with_columns(pl.col("bus_stop_time").cast(pl.Datetime))

        trips.append(df_trips)

    df_trips_all = pl.concat(trips)

    if is_schedule:

        # account for a bug detailed in github issue #21
        df_trips_all = df_trips_all.filter(pl.col("bus_stop_time").is_not_null())

        df_trips_all = df_trips_all.sort(["schd_trip_id", "bus_stop_time"])

        df_trips_all = df_trips_all.with_columns(
            total_stops=pl.col("stop_sequence")
            .cast(pl.Float64)
            .cast(pl.Int32)
            .max()
            .over("schd_trip_id"),
            trip_rn=pl.col("bus_stop_time").rank("ordinal").over("schd_trip_id"),
        )

        # create unique trip id. schd_trip_id is reused so count how many
        #  stops there are for a schd_trip_id (always the same) and use that
        # to determine when a trip ends and a new begins for trips with the
        # same schd_trip_id. tried to use num of stops / seq max for a
        # pattern but its not a unique value :(

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
            }
        )

    return df_trips_all


def group_metrics(trips_df: pl.DataFrame, metric: str):
    """
    Given a metric and a trips dataframe, this function will group the data by hour, day, week, month and year.
    """

    if "trip_duration" in metric:
        groupings = ["rt", "pid"]
    else:
        groupings = ["rt", "pid", "stop_id"]

    all_periods = []
    for grouping, trunc in [
        ("hour", "1h"),
        ("weekday", "1d"),
        ("month", "1mo"),
        ("year", "1y"),
        ("week_abs", "1w"),
        ("month_abs", "1mo"),
    ]:

        group_list = groupings.copy()
        group_list.append(grouping)

        if "num_buses" in metric:
            df = trips_df.with_columns(
                pl.col("bus_stop_time").dt.truncate(trunc).alias(grouping)
            )

            df = df.group_by([*group_list]).agg(
                pl.col("bus_stop_time").count().alias(metric)
            )

            if grouping == "hour":
                df = df.with_columns((pl.col(grouping).dt.hour()).alias(grouping))
            elif grouping == "weekday":
                df = df.with_columns((pl.col(grouping).dt.weekday()).alias(grouping))
            elif grouping == "month":
                df = df.with_columns((pl.col(grouping).dt.month()).alias(grouping))
            elif grouping == "year":
                df = df.with_columns((pl.col(grouping).dt.year()).alias(grouping))

        elif "trip_duration" in metric:
            df = trips_df.with_columns(
                (pl.col("start_trip").dt.hour()).alias("hour"),
                (pl.col("start_trip").dt.month()).alias("month"),
                (pl.col("start_trip").dt.year()).alias("year"),
                (pl.col("start_trip").dt.weekday()).alias("weekday"),
                (pl.col("start_trip").dt.truncate(trunc).alias(grouping)).alias(
                    "week_abs"
                ),
                (pl.col("start_trip").dt.truncate(trunc).alias(grouping)).alias(
                    "month_abs"
                ),
            )
        else:
            df = trips_df.with_columns(
                (pl.col("bus_stop_time").dt.hour()).alias("hour"),
                (pl.col("bus_stop_time").dt.month()).alias("month"),
                (pl.col("bus_stop_time").dt.year()).alias("year"),
                (pl.col("bus_stop_time").dt.weekday()).alias("weekday"),
                (pl.col("bus_stop_time").dt.truncate(trunc).alias(grouping)).alias(
                    "week_abs"
                ),
                (pl.col("bus_stop_time").dt.truncate(trunc).alias(grouping)).alias(
                    "month_abs"
                ),
            )

        grouped_df = df.group_by([*group_list]).agg(
            pl.count(metric).alias(f"count_{metric}"),
            pl.median(metric).alias(f"median_{metric}"),
            # pl.mean(metric).alias(f"mean_{metric}"),
            # pl.max(metric).alias(f"max_{metric}"),
            # pl.min(metric).alias(f"min_{metric}"),
            # pl.std(metric).alias(f"std_{metric}"),
            pl.col(metric).quantile(0.25).alias(f"q25_{metric}"),
            pl.col(metric).quantile(0.75).alias(f"q75_{metric}"),
        )

        grouped_df = grouped_df.with_columns((pl.lit(grouping).alias("period")))

        grouped_df = grouped_df.rename({grouping: "period_value"})

        # retype period value as string
        grouped_df = grouped_df.with_columns(pl.col("period_value").cast(pl.String))

        all_periods.append(grouped_df)

        all_periods_df = pl.concat(all_periods)

    return all_periods_df


def create_trips_df_pid(
    pid: str,
) -> pl.DataFrame:
    """
    prep routes for one pid
    """
    # just look at the rts for schedule

    file_DIR = f"{DIR}/processed_by_pid/trips_{pid}_full.parquet"
    error = f"Do not have pattern {pid} for route. Skipping"

    try:
        df_trips = pl.read_parquet(file_DIR)
    except FileNotFoundError:
        print(error)

    if "stop_dist" in df_trips.columns:
        df_trips = df_trips.drop("stop_dist")

    df_trips = df_trips.with_columns(pl.exclude("bus_stop_time").cast(pl.String))

    df_trips = df_trips.with_columns(
        pl.col("pid").cast(pl.Float64).cast(pl.Int32).cast(pl.String).alias("pid")
    )
    df_trips = df_trips.rename(
        {
            "unique_trip_vehicle_day": "trip_id",
            "stpid": "stop_id",
        }
    )

    return df_trips
