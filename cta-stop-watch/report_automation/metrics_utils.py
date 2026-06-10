import polars as pl
import pandas as pd
import pathlib
from utils import metrics_logger
from datetime import date, timedelta

# Constants -------------------------------------------------------------------

DIR = pathlib.Path(__file__).parent / "data"

# Only the columns needed downstream — avoids loading seg_combined, typ,
# speed_mph, p_stp_id, vid, stop_sequence, stop_dist from every parquet file.
_ACTUAL_COLS = ["bus_stop_time", "unique_trip_vehicle_day", "stpid", "rt", "pid"]
_SCHED_COLS  = ["bus_stop_time", "schd_trip_id", "stop_id", "route_id", "pid", "stop_sequence"]

# Functions -------------------------------------------------------------------


def create_trips_df(rt: str, is_schedule: bool = False) -> pl.DataFrame:
    """
    Given rts to pids xwalk and a list of rts, create a df for all the trips
    for the rts. Uses scan_parquet + column pruning and collects with the
    streaming engine to reduce peak memory. Window functions that follow
    require an eager DataFrame, so we collect before returning.
    """
    xwalk = pd.read_parquet(f"{DIR}/rt_to_pid.parquet")
    lazy_frames = []

    if is_schedule:
        file_path = f"{DIR}/clean_timetables/rt{rt}_timetable.parquet"
        try:
            lf = pl.scan_parquet(file_path)
            schema_cols = set(lf.collect_schema().names())
            select_cols = [c for c in _SCHED_COLS if c in schema_cols]
            lf = lf.select(select_cols).with_columns(
                pl.col("bus_stop_time").cast(pl.Datetime)
            )
            lazy_frames.append(lf)
        except FileNotFoundError:
            metrics_logger.debug(f"Do not have timetable for route {rt}. Skipping")
            return pl.DataFrame()
    else:
        data_set = xwalk[xwalk["rt"] == rt].groupby(["rt", "pid"]).count().reset_index()
        for obj in data_set.itertuples(index=False):
            pid = obj.pid
            file_path = f"{DIR}/processed_by_pid/trips_{pid}_full.parquet"
            try:
                lf = pl.scan_parquet(file_path)
                schema_cols = set(lf.collect_schema().names())
                select_cols = [c for c in _ACTUAL_COLS if c in schema_cols]
                lf = (
                    lf.select(select_cols)
                    .with_columns(
                        pl.col("bus_stop_time").cast(pl.Datetime),
                        pl.col("pid").cast(pl.Float64).cast(pl.Int32).cast(pl.String),
                        pl.col("unique_trip_vehicle_day").cast(pl.String),
                        pl.col("stpid").cast(pl.String),
                        pl.col("rt").cast(pl.String),
                    )
                )
                lazy_frames.append(lf)
            except FileNotFoundError:
                metrics_logger.debug(f"Do not have pattern {pid} for route. Skipping")
                continue

    if not lazy_frames:
        return pl.DataFrame()

    # Collect with the streaming engine — reduces peak memory vs. eager concat.
    # Window functions (.over, rank, map_elements) downstream require an eager DataFrame.
    df_trips_all = pl.concat(lazy_frames).collect(engine="streaming")

    min_date = df_trips_all.select(pl.min("bus_stop_time"))[0, 0].strftime("%Y-%m-%d")
    max_date = df_trips_all.select(pl.max("bus_stop_time"))[0, 0].strftime("%Y-%m-%d")
    metrics_logger.debug(f"Loaded bus trips ranging from {min_date} to {max_date}")
    print(f"        Bus trips ranging from {min_date} to {max_date}")

    if is_schedule:
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


def group_metrics(trips_df: pl.DataFrame, metric: str) -> pl.DataFrame:
    """
    Given a metric and a trips dataframe, group by hour, day, week, month and
    year. Uses lazy evaluation with the streaming engine for all group_by/agg
    steps to reduce peak memory. Concat is done once at the end (not inside
    the loop) to avoid O(n²) intermediate allocations.
    """

    if "trip_duration" in metric:
        groupings = ["rt", "pid"]
    else:
        groupings = ["rt", "pid", "stop_id"]

    all_periods = []

    last_month = (date.today().replace(day=1) - timedelta(days=1)).replace(day=1)

    for name, grouping, trunc in [
        ("hour", "hour", "1h"),
        ("weekday", "weekday", "1d"),
        ("month", "month", "1mo"),
        ("year", "year", "1y"),
        ("month_abs", "month_abs", "1mo"),
        ("year_hour", ["year", "hour"], "1h"),
        ("year_weekday", ["year", "weekday"], "1d"),
        ("last_full_month", "last_full_month", "something"),
    ]:

        group_list = groupings.copy()
        if isinstance(grouping, list):
            group_list = group_list + grouping
        else:
            group_list.append(grouping)

        if "num_buses" in metric:
            if isinstance(grouping, list):
                trunc_name = grouping[1]
            else:
                trunc_name = grouping

            if grouping == "last_full_month":
                df = trips_df.filter(
                    pl.col("bus_stop_time").dt.year() == last_month.year
                ).filter(pl.col("bus_stop_time").dt.month() == last_month.month)
                df = df.with_columns(pl.lit(grouping).alias(grouping))
            else:
                df = trips_df.with_columns(
                    pl.col("bus_stop_time").dt.truncate(trunc).alias(trunc_name)
                )

            df = (
                df.lazy()
                .group_by([*group_list])
                .agg(pl.col("bus_stop_time").count().alias(metric))
                .collect(engine="streaming")
            )

            if grouping == "hour":
                df = df.with_columns((pl.col(trunc_name).dt.hour()).alias(name))
            elif grouping == "weekday":
                df = df.with_columns((pl.col(trunc_name).dt.weekday()).alias(name))
            elif grouping == "month":
                df = df.with_columns((pl.col(trunc_name).dt.month()).alias(name))
            elif grouping == "year":
                df = df.with_columns((pl.col(trunc_name).dt.year()).alias(name))
            elif grouping == ["year", "hour"]:
                df = df.with_columns(
                    pl.concat_str(
                        [pl.col(trunc_name).dt.year(), pl.col(trunc_name).dt.hour()],
                        separator="-",
                    ).alias(name),
                )
            elif grouping == ["year", "weekday"]:
                df = df.with_columns(
                    pl.concat_str(
                        [pl.col(trunc_name).dt.year(), pl.col(trunc_name).dt.weekday()],
                        separator="-",
                    ).alias(name),
                )
            elif grouping == "last_full_month":
                pass

        else:
            if "trip_duration" in metric:
                time_col = "start_trip"
            else:
                time_col = "bus_stop_time"

            if grouping == "hour":
                df = trips_df.with_columns((pl.col(time_col).dt.hour()).alias(name))
            elif grouping == "weekday":
                df = trips_df.with_columns((pl.col(time_col).dt.weekday()).alias(name))
            elif grouping == "month":
                df = trips_df.with_columns((pl.col(time_col).dt.month()).alias(name))
            elif grouping == "year":
                df = trips_df.with_columns((pl.col(time_col).dt.year()).alias(name))
            elif grouping == "month_abs":
                df = trips_df.with_columns(
                    pl.col(time_col).dt.truncate(trunc).alias("month_abs")
                )
            elif grouping == "week_abs":
                df = trips_df.with_columns(
                    pl.col(time_col).dt.truncate(trunc).alias("week_abs")
                )
            elif grouping == ["year", "hour"]:
                df = trips_df.with_columns(
                    pl.concat_str(
                        [pl.col(time_col).dt.year(), pl.col(time_col).dt.hour()],
                        separator="-",
                    ).alias(name),
                )
            elif grouping == ["year", "weekday"]:
                df = trips_df.with_columns(
                    pl.concat_str(
                        [pl.col(time_col).dt.year(), pl.col(time_col).dt.weekday()],
                        separator="-",
                    ).alias(name),
                )
            elif grouping == "last_full_month":
                df = trips_df.filter(
                    pl.col("bus_stop_time").dt.year() == last_month.year
                ).filter(pl.col("bus_stop_time").dt.month() == last_month.month)
                df = df.with_columns(pl.lit(grouping).alias(grouping))

        final_group = groupings.copy()
        final_group.append(name)

        grouped_df = (
            df.lazy()
            .group_by([*final_group])
            .agg(
                pl.count(metric).alias(f"count_{metric}"),
                pl.median(metric).alias(f"median_{metric}"),
                pl.col(metric).quantile(0.25).alias(f"q25_{metric}"),
                pl.col(metric).quantile(0.75).alias(f"q75_{metric}"),
            )
            .with_columns(pl.lit(name).alias("period"))
            .rename({name: "period_value"})
            .with_columns(pl.col("period_value").cast(pl.String))
            .collect(engine="streaming")
        )

        all_periods.append(grouped_df)

    # Concat once at the end — avoids O(n²) intermediate allocations
    return pl.concat(all_periods)


def create_trips_df_pid(pid: str) -> pl.DataFrame:
    """
    Prep routes for one pid. Uses scan_parquet for lazy reading.
    """
    file_path = f"{DIR}/processed_by_pid/trips_{pid}_full.parquet"

    try:
        lf = pl.scan_parquet(file_path)
        schema_cols = set(lf.collect_schema().names())
        select_cols = [c for c in _ACTUAL_COLS if c in schema_cols]
        df_trips = (
            lf.select(select_cols)
            .with_columns(
                pl.col("bus_stop_time").cast(pl.Datetime),
                pl.col("pid").cast(pl.Float64).cast(pl.Int32).cast(pl.String),
                pl.col("unique_trip_vehicle_day").cast(pl.String),
                pl.col("stpid").cast(pl.String),
                pl.col("rt").cast(pl.String),
            )
            .collect(engine="streaming")
        )
    except FileNotFoundError:
        print(f"Do not have pattern {pid} for route. Skipping")
        return pl.DataFrame()

    return df_trips.rename(
        {
            "unique_trip_vehicle_day": "trip_id",
            "stpid": "stop_id",
        }
    )


# End -------------------------------------------------------------------------
