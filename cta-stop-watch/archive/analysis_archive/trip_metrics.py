import polars as pl
from utils import group_metrics


def get_trip_duration(trip_df):
    """ """
    trip_duration_stg = trip_df.group_by(["trip_id", "rt", "pid"]).agg(
        pl.min("bus_stop_time").alias("start_trip"),
        pl.max("bus_stop_time").alias("end_trip"),
    )
    trip_duration = trip_duration_stg.with_columns(
        (trip_duration_stg["end_trip"] - trip_duration_stg["start_trip"]).alias(
            "trip_duration"
        )
    )
    return trip_duration


def create_trips_metric_df(trip_df, is_schedule: bool):

    trip_duration_df = get_trip_duration(trip_df)
    if is_schedule:
        rename_dict = {
            "trip_duration": "schedule_trip_duration",
        }
    else:
        rename_dict = {
            "trip_duration": "actual_trip_duration",
        }
    trip_duration_df = trip_duration_df.rename(rename_dict)

    final_df = group_metrics(trip_duration_df, rename_dict["trip_duration"])

    return final_df


def create_combined_metrics_trip_df(scheduled_df, actual_df):
    """
    For all the routes, create a combined DataFrame with all the metrics for both scheduled and actual data.
    """

    combined_df = scheduled_df.join(
        actual_df,
        on=["rt", "pid", "period", "period_value"],
        how="full",
        coalesce=True,
    )
    # For the combined DataFrame calculate the average delay for till the next bus arrives at a given bus stop
    # combined_df = combined_df.with_columns(
    #     time_till_next_bus_delay=(
    #         pl.col("median_actual_time_till_next_bus")
    #         - pl.col("median_schedule_time_till_next_bus")
    #     )
    # )

    return combined_df
