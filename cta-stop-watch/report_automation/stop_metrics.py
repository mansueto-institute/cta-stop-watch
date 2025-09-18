import polars as pl
from metrics_utils import group_metrics
from utils import metrics_logger

# Functions -------------------------------------------------------------------


def time_to_next_stop(
    trips_df: pl.DataFrame,
    is_daytime: bool = True,
) -> pl.DataFrame:
    """
    calculate time to next stop and other metrics for each bus stop
    """

    metrics_logger.debug("Computing time to next stop")

    if is_daytime:
        trips_df = trips_df.filter(pl.col("bus_stop_time").dt.hour().is_between(6, 20))

    # finds time between buses at each stop for a given route
    trips_df = trips_df.sort(["stop_id", "bus_stop_time"])
    trips_df = trips_df.with_columns(
        time_till_next_bus=(
            pl.col("bus_stop_time").shift(-1) - pl.col("bus_stop_time")
        ).over(pl.col("stop_id").rle_id())
    )

    # finds time between bus stops for a given route
    trips_df = trips_df.sort(by=["trip_id", "bus_stop_time"])
    trips_df = trips_df.with_columns(
        time_to_previous_stop=(
            pl.col("bus_stop_time") - pl.col("bus_stop_time").shift(1)
        ).over(pl.col("trip_id").rle_id())
    )

    # convert duration to seconds as in interger
    trips_df = trips_df.with_columns(
        time_to_previous_stop=pl.col("time_to_previous_stop").dt.total_seconds(),
        time_till_next_bus=pl.col("time_till_next_bus").dt.total_seconds(),
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

    missing_df = trips_df.null_count().unpivot(
        index="rt", variable_name="metric", value_name="missing_count"
    )
    with pl.Config(tbl_rows=20):
        metrics_logger.debug(f"Missing values:\n{missing_df}")

    return trips_df


def join_metrics(all_metrics: list[pl.DataFrame]) -> pl.DataFrame:
    """
    Takes a list of data frames with metrics and joins them into a single one
    """

    for i, _ in enumerate(all_metrics):
        if i == 0:
            static = all_metrics[i]
            continue

        static = static.join(
            all_metrics[i],
            on=["rt", "pid", "stop_id", "period", "period_value"],
            how="full",
            coalesce=True,
        )

    return static


def create_route_metrics_df(route_df: pl.DataFrame, is_schedule: bool) -> pl.DataFrame:
    """
    create stop metrics for one route
    """

    metrics_logger.debug("\n\nCompute metrics")
    trips_df = time_to_next_stop(route_df)

    if is_schedule:
        metrics_logger.debug("\nCreating route SCHEDULE metrics DataFrame")
        trips_df = trips_df.rename(
            {
                "time_till_next_bus": "schedule_time_till_next_bus",
                "time_to_previous_stop": "schedule_time_to_previous_stop",
                "cum_trip_time": "schedule_cum_trip_time",
            }
        )
    else:
        metrics_logger.debug("\nCreating route ACTUAL metrics DataFrame")
        trips_df = trips_df.rename(
            {
                "time_till_next_bus": "actual_time_till_next_bus",
                "time_to_previous_stop": "actual_time_to_previous_stop",
                "cum_trip_time": "actual_cum_trip_time",
            }
        )

    # find grouped metrics for depending on actual or schedule
    metrics_logger.debug("\n\nJoin route metrics")
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

    one_route = join_metrics(all_metrics)

    missing_df = one_route.null_count().unpivot(
        index="rt", variable_name="metric", value_name="missing_count"
    )
    with pl.Config(tbl_rows=20):
        metrics_logger.debug(f"Missing values on route metrics:\n{missing_df}")

    return one_route


def create_combined_metrics_stop_df(
    scheduled_df: pl.DataFrame, actual_df: pl.DataFrame
) -> pl.DataFrame:
    """
    For all the routes, create a combined DataFrame with all the metrics for both scheduled and actual data.
    """

    combined_df = scheduled_df.join(
        actual_df,
        on=["rt", "pid", "stop_id", "period", "period_value"],
        how="full",
        coalesce=True,
    )
    # For the combined DataFrame calculate the average delay for till the next bus arrives at a given bus stop
    combined_df = combined_df.with_columns(
        time_till_next_bus_delay=(
            pl.col("median_actual_time_till_next_bus")
            - pl.col("median_schedule_time_till_next_bus")
        )
    )

    return combined_df


# End -------------------------------------------------------------------------
