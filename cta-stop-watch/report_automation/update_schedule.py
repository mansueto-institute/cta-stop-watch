import pathlib
import warnings

warnings.simplefilter(action="ignore", category=FutureWarning)

import pandas as pd
import gtfs_kit as gk
import pathlib
import os
import duckdb
import sys
import numpy as np
import urllib.request
from datetime import date


def download_current_feed():

    today = str(date.today())

    URL = "https://www.transitchicago.com/downloads/sch_data/google_transit.zip"
    urllib.request.urlretrieve(URL, f"data/staging/timetables/feed_{today}.zip")

    return True


def process_route_timetable(
    feed: "Feed", route_id: str, dates: list[str], merged_df: pd.DataFrame, a
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


def create_timetables():
    """
    creates a time table from a feed for each route using the process_route_timetable
    function, an adaption of gtfs_kit.build_route_timetable
    """
    today = str(date.today())
    file = f"feed_{today}.zip"

    file_path = pathlib.Path(__file__).parent / f"data/staging/timetables/{file}"

    feed = gk.read_feed(file_path, dist_units="m")  # in meters
    rts = feed.routes[feed.routes["route_short_name"].notna()]["route_id"]
    all_dates = feed.get_dates()

    rts_count = 0

    # run full feed tables once and feed into process_route_timetable
    merged_df = pd.merge(feed.trips, feed.stop_times)
    a = feed.compute_trip_activity(all_dates)

    for rt in rts:
        print(f"creating timetable for route {rt}")

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

        timetables_df["sha1"] = None
        timetables_df["fetched_date"] = today

        rts_count += 1

        if rts_count % 40 == 0:
            print(f"{round((rts_count/len(rts)) * 100,3)} complete")

        if not os.path.exists("data/staging/timetables/current_timetables"):
            os.makedirs("data/staging/timetables/current_timetables")

        print("Writing to parquet file")
        timetables_df.to_parquet(
            f"data/staging/timetables/current_timetables/rt{rt}_timetable.parquet",
            index=False,
        )

    return True


def dedupe_schedules():
    """
    given all the historic schedules, dedupe them by date and time by taking only
    scheduled trips from a schedule in which there was not an update

    """
    # get all route
    rt_DIR = pathlib.Path(__file__).parent / ("data/rt_to_pid.parquet")
    rt_to_pid = pd.read_parquet(rt_DIR)
    rts = rt_to_pid["rt"].unique()

    # testing
    rts = [21]

    for rt in rts:

        print(f"De-duping timetable for route {rt}")

        # find min date of current schedule
        current = pd.read_parquet(
            f"data/staging/timetables/current_timetables/rt{rt}_timetable.parquet"
        )

        current["date_type"] = pd.to_datetime(current["date"])

        min_date = current["date_type"].min()

        # for historic schedule, remove anything never than this date
        historic = pd.read_parquet(f"data/clean_timetables/rt{rt}_timetable.parquet")
        historic = historic[historic["bus_stop_time"] < min_date]

        # issue with this documented in issue #21
        # create an actual bus_stop_time column using date and arrival_time
        current["time_edit"] = np.where(
            current["arrival_time"].str.slice(0, 2) == "24",
            current["arrival_time"].str.replace("24", "00"),
            current["arrival_time"],
        )
        current["date_edit"] = np.where(
            current["arrival_time"].str.slice(0, 2) == "24",
            (
                pd.to_datetime(current["date"], format="%Y%m%d")
                + 1 * pd.Timedelta(days=1)
            ),
            pd.to_datetime(current["date"]),
        )
        current["bus_stop_time"] = pd.to_datetime(
            current["date_edit"].astype(str) + current["time_edit"],
            format="%Y-%m-%d%H:%M:%S",
            errors="coerce",
        )

        current.drop(
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

        # combine current and historic
        deduped_timetable = pd.concat([current, historic])

        # convert stop seq to str
        deduped_timetable["stop_sequence"] = deduped_timetable["stop_sequence"].astype(
            str
        )

        deduped_timetable.to_parquet(f"data/clean_timetables/rt{rt}_timetable.parquet")


def update_schedule():
    pass
    # download current schedule
    download_current_feed()

    # create time table of current schedule
    create_timetables()

    # dedupe with historic schedule
    dedupe_schedules()
