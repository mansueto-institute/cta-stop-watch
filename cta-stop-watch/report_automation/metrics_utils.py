import polars as pl
import pandas as pd
import pathlib
from utils import metrics_logger
from datetime import date, timedelta

# Contants --------------------------------------------------------------------

# Paths
DIR = pathlib.Path(__file__).parent / "data"

# Functions -------------------------------------------------------------------


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


def group_metrics(trips_df: pl.DataFrame, metric: str) -> pl.DataFrame:
    """
    Given a metric and a trips dataframe, this function will group the data by hour, day, week, month and year.
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
        # ("week_abs", "1w"),
        ("month_abs", "month_abs", "1mo"),
        # adding more groupings
        ("year_hour", ["year", "hour"], "1h"),  # year hour
        ("year_weekday", ["year", "weekday"], "1d"),  # year weekday
        ("last_full_month", "last_full_month", "something"),  # last month
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

            df = df.group_by([*group_list]).agg(
                pl.col("bus_stop_time").count().alias(metric)
            )

            # this becomes the period_value
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
                # already added this so can
                pass

        else:
            if "trip_duration" in metric:
                time_col = "start_trip"
            else:
                time_col = "bus_stop_time"

            # this becomes the period_value
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

        grouped_df = df.group_by([*final_group]).agg(
            pl.count(metric).alias(f"count_{metric}"),
            pl.median(metric).alias(f"median_{metric}"),
            pl.col(metric).quantile(0.25).alias(f"q25_{metric}"),
            pl.col(metric).quantile(0.75).alias(f"q75_{metric}"),
        )

        grouped_df = grouped_df.with_columns((pl.lit(name).alias("period")))

        grouped_df = grouped_df.rename({name: "period_value"})

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


# End -------------------------------------------------------------------------
