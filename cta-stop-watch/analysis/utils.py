import polars as pl


def create_trips_df(xwalk: pl.DataFrame, rts: list, is_scheduled: bool = False):
    """
    Given rts to pids xwalk and a list of rts, create a df for all the trips for the rts
    """

    trips = []

    # just look at the rts for schedule
    if is_scheduled:
        iter = rts
        file_DIR = "../cta-stop-etl/out/clean_timetables/rt{rt}_timetable.parquet"
        error = "Do not have timetable for route {rt}. Skipping"
    else:
        pids = xwalk.filter(pl.col("rt").is_in(rts)).unique("pid")
        rt_pid = pids.rows()
        iter = rt_pid

        file_DIR = "../cta-stop-etl/out/trips/trips_{pid}_full.parquet"
        error = "Do not have pattern {pid} for route. Skipping"

    for obj in iter:
        if is_scheduled:
            rt = obj
            template_values = {"rt": rt}

        else:
            rt, pid = obj
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
            {"unique_trip_vehicle_day": "trip_id", "stpid": "stop_id"}
        )

    return df_trips_all
