import polars as pl
from utils import group_metrics


def time_to_next_stop(
    trips_df: pl.DataFrame,
    is_daytime: bool = True,
):

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


def join_metrics(all_metrics: list[pl.DataFrame]):

    for i, df in enumerate(all_metrics):
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


def create_route_metrics_df(route_df, is_schedule: bool):
    """
    create stop metrics for one route
    """

    trips_df = time_to_next_stop(route_df)
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

    one_route = join_metrics(all_metrics)

    return one_route


def create_combined_metrics_stop_df(scheduled_df, actual_df):
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
