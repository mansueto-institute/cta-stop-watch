import polars as pl
import pandas as pd


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


def create_trips_df(rt: str, is_schedule: bool = False) -> pl.DataFrame:
    """
    Given rts to pids xwalk and a list of rts, create a df for all the trips for the rts
    """

    trips = []
    xwalk = pd.read_parquet("rt_to_pid.parquet")

    # just look at the rts for schedule
    if is_schedule:
        iter = rt
        file_DIR = "../cta-stop-etl/out/clean_timetables/rt{rt}_timetable.parquet"
        error = "Do not have timetable for route {rt}. Skipping"
    else:
        data_set = xwalk[xwalk["rt"] == rt].groupby(["rt", "pid"]).count().reset_index()
        pids = data_set.itertuples(index=False)
        iter = pids

        file_DIR = "../cta-stop-etl/out/trips/trips_{pid}_full.parquet"
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
            print(error.format(**template_values))
            continue

        if not is_schedule:
            # for now
            df_trips = df_trips.filter(pl.col("typ") == "S")
            df_trips = df_trips.with_columns(
                pl.lit(pid).cast(pl.Int64).alias("pid"),
                pl.lit(rt).cast(pl.String).alias("rt"),
            )

        trips.append(df_trips)

    df_trips_all = pl.concat(trips)

    if is_schedule:
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

# def time_till_next_bus(
#     trips_df: pl.DataFrame,
#     is_daytime: bool = True,
# ):
#     if is_daytime:
#         trips_df = trips_df.filter(pl.col("bus_stop_time").dt.hour().is_between(6, 20))

#     trips_df = trips_df.sort(['stop_id', 'bus_stop_time'])
#     trips_df = trips_df.with_columns([
#         (pl.col('bus_stop_time').diff().over(['stop_id']).dt.total_seconds() / 60).alias('time_till_next_bus')
#     ])

#     return trips_df

def time_to_next_stop(
    trips_df: pl.DataFrame,
    is_daytime: bool = True,
):

    if is_daytime:
        trips_df = trips_df.filter(pl.col("bus_stop_time").dt.hour().is_between(6, 20))

    # finds time between buses at each stop for a given route
    trips_df = trips_df.sort(['stop_id', 'bus_stop_time'])
    trips_df = trips_df.with_columns([
        (pl.col('bus_stop_time').diff().over(['stop_id']).dt.total_seconds() / 60).alias('time_till_next_bus')
    ])

    # finds time between bus stops for a given route 
    trips_df = trips_df.sort(by=["trip_id", "bus_stop_time"])
    trips_df = trips_df.with_columns(
        time_to_previous_stop=(
            pl.col("bus_stop_time") - pl.col("bus_stop_time").shift(1)
        ).over(pl.col("trip_id").rle_id())
    )

    trips_df = trips_df.with_columns(
        pl.col("time_to_previous_stop").fill_null(strategy="zero")
    )

    # finds the cumulative time for a trip for a given route 
    trips_df = trips_df.with_columns(
        cum_trip_time=pl.cum_sum("time_to_previous_stop").over(
            pl.col("trip_id").rle_id()
        )
    )

    trips_df = trips_df.with_columns(
        (pl.col("bus_stop_time").dt.hour()).alias("hour"),
        (pl.col("bus_stop_time").dt.month()).alias("month"),
        (pl.col("bus_stop_time").dt.year()).alias("year"),
        (pl.col("bus_stop_time").dt.day()).alias("day"),
    )

    return trips_df


def group_metrics(trips_df: pl.DataFrame, metric: str):
    """
    Given a metric and a trips dataframe, this function will group the data by hour, day, week, month and year.
    """

    all_periods = []
    for grouping, trunc in [
        ("hour", "1h"),
        ("weekday", "1d"),
        ("dayofyear", "1d"),
        ("week", "1w"),
        ("month", "1mo"),
        ("year", "1y"),
    ]:

        group_list = ["rt", "pid", "stop_sequence", "stop_id", grouping]

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
            elif grouping == "dayofyear":
                df = df.with_columns(
                    (pl.col(grouping).dt.ordinal_day()).alias(grouping)
                )
            elif grouping == "week":
                df = df.with_columns((pl.col(grouping).dt.week()).alias(grouping))
            elif grouping == "month":
                df = df.with_columns((pl.col(grouping).dt.month()).alias(grouping))
            elif grouping == "year":
                df = df.with_columns((pl.col(grouping).dt.year()).alias(grouping))

        else:
            df = trips_df.with_columns(
                (pl.col("bus_stop_time").dt.hour()).alias("hour"),
                (pl.col("bus_stop_time").dt.month()).alias("month"),
                (pl.col("bus_stop_time").dt.year()).alias("year"),
                (pl.col("bus_stop_time").dt.weekday()).alias("weekday"),
                (pl.col("bus_stop_time").dt.ordinal_day()).alias("dayofyear"),
                (pl.col("bus_stop_time").dt.week()).alias("week"),
            )

        grouped_df = df.group_by([*group_list]).agg(
            pl.median(metric).alias(f"median_{metric}"),
            pl.mean(metric).alias(f"mean_{metric}"),
            pl.max(metric).alias(f"max_{metric}"),
            pl.min(metric).alias(f"min_{metric}"),
            pl.std(metric).alias(f"std_{metric}"),
            pl.col(metric).quantile(0.25).alias(f"q25_{metric}"),
            pl.col(metric).quantile(0.75).alias(f"q75_{metric}"),
        )

        grouped_df = grouped_df.with_columns((pl.lit(grouping).alias("period")))

        grouped_df = grouped_df.rename({grouping: "period_value"})

        grouped_df = grouped_df.with_columns(pl.col("period_value").cast(pl.Int32))

        all_periods.append(grouped_df)

        all_periods_df = pl.concat(all_periods)

    return all_periods_df


def join_metrics(all_metrics: list[pl.DataFrame]):

    for i, df in enumerate(all_metrics):
        if i == 0:
            static = all_metrics[i]
            continue

        static = static.join(
            all_metrics[i],
            on=["rt", "pid", "stop_id", "stop_sequence", "period", "period_value"],
            how="full",
            coalesce=True,
        )

    return static


def create_all_metrics_df(rts: list | str, is_schedule: bool):
    """
    for all the routes, create a df with all the metrics
    """
    if rts == "all":
        xwalk = pd.read_parquet("rt_to_pid.parquet")
        rts = xwalk["rt"].unique().tolist()

    one_route = []
    for rt in rts:
        print(f"Processing route {rt}")
        try:
            trips_df = create_trips_df(rt=rt, is_schedule=is_schedule)
        except:
            print(f"issue with rt {rt}")
            continue

        trips_df = time_to_next_stop(trips_df)
        if is_schedule:
            trips_df = trips_df.rename(
                {
                    "time_till_next_bus": "schedule_time_till_next_bus",
                    "time_to_previous_stop": "schedule_time_to_previous_stop",
                    "cum_trip_time": "schedule_cum_trip_time",
                }
            )
        else:
            trips_df = trips_df.rename(
                {
                    "time_till_next_bus": "actual_time_till_next_bus",
                    "time_to_previous_stop": "actual_time_to_previous_stop",
                    "cum_trip_time": "actual_cum_trip_time",
                }
            )
        
        # find grouped metrics for depending on actual or schedule 
        all_metrics = []
        metrics = [
            "time_till_next_bus",
            "time_to_previous_stop",
            "cum_trip_time",
            "num_buses",
        ]
        metrics = [f"schedule_{m}" if is_schedule else f"actual_{m}" for m in metrics]

        for metric in metrics:
            grouped = group_metrics(trips_df, metric)
            all_metrics.append(grouped)
        
        one_route.append(join_metrics(all_metrics))

    all_df = pl.concat(one_route)

    return all_df

def create_combined_metrics_df(rts: list | str) -> pl.DataFrame:
    """
    For all the routes, create a combined DataFrame with all the metrics for both scheduled and actual data.
    """
    scheduled_df = create_all_metrics_df(rts, is_schedule=True)
    actual_df = create_all_metrics_df(rts, is_schedule=False)

    combined_df = scheduled_df.join(actual_df,
                                on=["rt", "pid", "stop_id", "stop_sequence", "period", "period_value"],
                                how="full",
                                coalesce=True,
                            )
    # For the combined DataFrame calculate the average delay for till the next bus arrives at a given bus stop
    combined_df = combined_df.with_columns(
        delay_avg=(pl.col('median_actual_time_till_next_bus') - pl.col('median_schedule_time_till_next_bus')).alias('avg_time_till_next_bus_delay')
    )

    return combined_df

# def average_delay(combined_df: pl.DataFrame): 
#     """
#     For the combined DataFrame calculate the average delay for till the next bus arrives at a given bus stop.
#     """ 
#     combined_df = combined_df.with_columns(
#         delay_avg=(pl.col('median_actual_time_till_next_bus') - pl.col('median_schedule_time_till_next_bus')).alias('avg_time_till_next_bus_delay')
#     )

#     return combined_df