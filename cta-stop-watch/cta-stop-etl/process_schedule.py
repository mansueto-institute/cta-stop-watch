import pathlib
import warnings
import pandas as pd
import gtfs_kit as gk
import os
import duckdb
import sys
import numpy as np
import logging

warnings.simplefilter(action="ignore", category=FutureWarning)

# Contants --------------------------------------------------------------------

# Paths
DIR = pathlib.Path(__file__).parent / "../scrapers/inp/historic_gtfs"
rt_DIR = pathlib.Path(__file__).parent / ("../analysis/rt_to_pid.parquet")
tt_DIR = pathlib.Path(__file__).parent / ("out/timetables_raw/*.parquet")
finished_rts_path = pathlib.Path(__file__).parent / ("out/clean_timetables")
OUT = pathlib.Path(__file__).parent / "out/clean_timetables"

# Functions -------------------------------------------------------------------


def process_route_timetable(
    feed: gk.feed.Feed, route_id: str, dates: list[str], merged_df: pd.DataFrame, a
) -> pd.DataFrame:
    """
    Return a timetable for the given route and dates (YYYYMMDD date strings).

    Return a DataFrame with whose columns are all those in ``feed.trips`` plus those in
    ``feed.stop_times`` plus ``'date'``.
    The trip IDs are restricted to the given route ID.
    The result is sorted first by date and then by grouping by
    trip ID and sorting the groups by their first departure time.

    Skip dates outside of the Feed's dates.

    If there is no route activity on the given dates, then return
    an empty DataFrame.

    AS Notes: adapted from gtfs_kit.build_route_timetable to run multiple routes
    and removing redundant code. Documentation here:
    https://github.com/mrcagney/gtfs_kit/blob/master/gtfs_kit/routes.py#L643
    """
    dates = feed.subset_dates(dates)
    if not dates:
        return pd.DataFrame()

    t = merged_df[merged_df["route_id"] == route_id].copy()

    frames = []
    for date in dates:
        # Slice to trips active on date
        ids = a.loc[a[date] == 1, "trip_id"]
        f = t[t["trip_id"].isin(ids)].copy()
        f["date"] = date
        # Groupby trip ID and sort groups by their minimum departure time.
        # For some reason NaN departure times mess up the transform below.
        # So temporarily fill NaN departure times as a workaround.
        f["dt"] = f["departure_time"].fillna(method="ffill")
        f["min_dt"] = f.groupby("trip_id")["dt"].transform(min)
        frames.append(f)

    f = pd.concat(frames)

    return f.drop(["min_dt", "dt"], axis=1)


def create_timetables(max_feeds: int = 100) -> bool:
    """
    creates a time table from a feed for each route using the process_route_timetable
    function, an adaption of gtfs_kit.build_route_timetable
    """

    feed_count = 1

    version_dates_df = pd.read_parquet("out/historic_gtfs_version_dates.parquet")
    version_dates_df = version_dates_df[["sha1", "fetched_at"]]

    for path in os.listdir(DIR):
        if path.split(".")[1] != "zip":
            continue

        full_path = str(DIR) + "/" + path
        sha1 = path.split("/")[-1].split(".")[0]

        logging.info(f"Reading feed {sha1}")

        feed = gk.read_feed(full_path, dist_units="m")  # in meters
        rts = feed.routes[feed.routes["route_short_name"].notna()]["route_id"]
        all_dates = feed.get_dates()

        fetched_date = version_dates_df[version_dates_df["sha1"] == sha1][
            "fetched_at"
        ].values[0]

        one_feed_all_rts = []
        rts_count = 0

        # run full feed tables once and feed into process_route_timetable
        merged_df = pd.merge(feed.trips, feed.stop_times)
        a = feed.compute_trip_activity(all_dates)

        for rt in rts:
            logging.info(f"creating timetable for route {rt}")

            timetables_df = process_route_timetable(feed, rt, all_dates, merged_df, a)
            timetables_df["pid"] = timetables_df["shape_id"].str.slice(-5)

            timetables_df = timetables_df[
                [
                    "route_id",
                    "pid",
                    "schd_trip_id",
                    "stop_id",
                    "stop_sequence",
                    "date",
                    "arrival_time",
                    "departure_time",
                    "service_id",
                    "trip_id",
                ]
            ]

            timetables_df = timetables_df.to_dict(orient="records")
            one_feed_all_rts += timetables_df

            rts_count += 1

            if rts_count % 40 == 0:
                logging.info(f"{round((rts_count/len(rts)) * 100,3)} complete")

        logging.info("Merging all routes into one dataframe")

        one_feed_df = pd.DataFrame(one_feed_all_rts, dtype=str)
        one_feed_df["sha1"] = sha1
        one_feed_df["fetched_date"] = fetched_date

        if not os.path.exists("out/timetables_raw"):
            os.makedirs("out/timetables_raw")

        logging.info("Writing to parquet file")

        # one_feed_df.to_parquet(f"out/timetables_test/{sha1}.parquet", index=False)
        one_feed_df.to_parquet(f"out/timetables_raw/{sha1}.parquet", index=False)

        feed_count += 1
        if feed_count > max_feeds:
            break

    return True


def dedupe_schedules() -> None:
    """
    given all the historic schedules, dedupe them by date and time by taking only
    scheduled trips from a schedule in which there was not an update

    """
    # get all route
    rt_to_pid = pd.read_parquet(rt_DIR)
    rts = rt_to_pid["rt"].unique()

    rts_finished = []
    for file_name in os.listdir(finished_rts_path):
        route = file_name.split("_")[0][2:]
        rts_finished.append(route)

    # testing
    # rts = [1]

    for rt in rts:
        if str(rt) in rts_finished:
            logging.info(f"Route {rt} already finished")
            continue

        logging.info(f"De-duping timetable for route {rt}")
        # combine with duckdb
        cmd_number = f"""
        SELECT *
        FROM read_parquet('{tt_DIR}')
        WHERE route_id = '{str(rt)}'
        """

        df = duckdb.execute(cmd_number).df()

        df["date_type"] = pd.to_datetime(df["date"])

        dates = (
            df.groupby(["sha1", "fetched_date"])["date_type"]
            .min()
            .reset_index(name="min_date")
            .sort_values(by="fetched_date", ascending=False)
        )
        dates["max_date"] = dates["min_date"].shift(1)
        max_date_dict = dict(zip(dates.sha1, dates.max_date))

        deduped_timetable = (
            df.groupby("sha1")
            .apply(
                lambda g: g[
                    g["date_type"]
                    < pd.to_datetime(max_date_dict[g["sha1"].unique()[0]])
                ]
            )
            .reset_index(drop=True)
        )

        # create an actual bus_stop_time column using date and arrival_time
        deduped_timetable["time_edit"] = np.where(
            deduped_timetable["arrival_time"].str.slice(0, 2) == "24",
            deduped_timetable["arrival_time"].str.replace("24", "00"),
            deduped_timetable["arrival_time"],
        )
        deduped_timetable["date_edit"] = np.where(
            deduped_timetable["arrival_time"].str.slice(0, 2) == "24",
            (
                pd.to_datetime(deduped_timetable["date"], format="%Y%m%d")
                + 1 * pd.Timedelta(days=1)
            ),
            pd.to_datetime(deduped_timetable["date"]),
        )
        deduped_timetable["bus_stop_time"] = pd.to_datetime(
            deduped_timetable["date_edit"].astype(str) + deduped_timetable["time_edit"],
            format="%Y-%m-%d%H:%M:%S",
            errors="coerce",
        )

        deduped_timetable.drop(
            columns=[
                "date_type",
                "departure_time",
                "arrival_time",
                "sha1",
                "fetched_date",
                "time_edit",
                "date_edit",
                "date",
            ],
            inplace=True,
        )

        if not os.path.exists(f"{OUT}"):
            os.makedirs(f"{OUT}")

        deduped_timetable.to_parquet(f"out/clean_timetables/rt{rt}_timetable.parquet")


# Implementation --------------------------------------------------------------

if __name__ == "__main__":
    if sys.argv[1] == "--create_timetables":
        create_timetables()
    elif sys.argv[1] == "--dedupe":
        dedupe_schedules()

# END -------------------------------------------------------------------------
