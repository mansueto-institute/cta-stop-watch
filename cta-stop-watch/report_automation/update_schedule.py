# Libraries -------------------------------------------------------------------

import pathlib
import warnings
import http
import pandas as pd
import gtfs_kit as gk
import os
import duckdb
import numpy as np
import requests
from datetime import date
from utils import metrics_logger

warnings.simplefilter(action="ignore", category=FutureWarning)

# Constants -------------------------------------------------------------------

URL = "https://www.transitchicago.com/downloads/sch_data/google_transit.zip"

# Functions -------------------------------------------------------------------


def download_current_feed() -> bool:
    """
    Download the current gtfs feed from CTA
    """

    today = str(date.today())
    download_path = f"data/staging/timetables/feed_{today}.zip"

    # Ensure path exists
    os.makedirs(os.path.dirname(download_path), exist_ok=True)

    # Note: This approach reaad 8kb (8192b) at a time to avoid
    # facing the IncompleteRead error.
    try:
        with requests.get(URL, stream=True, timeout=60) as r:
            r.raise_for_status()
            with open(download_path, "wb") as f:
                for chunck in r.iter_content(chunck_size=8192):
                    if chunck:
                        f.write(chunck)
            return True
    except requests.exceptions.HTTPError as e:
        metrics_logger.error(
            f"Failed to download current schedules due to connection error. See full error: \n{e}"
        )
        raise e
    except http.client.IncompleteRead as e:
        metrics_logger.error(
            f"Failed to read current schedules file. See full error: \n{e}"
        )
        raise e
    except Exception as e:
        metrics_logger.error(f"Failed to get current schedules. See full error:\n{e}")
        raise e


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
    for day in dates:
        # Slice to trips active on date
        ids = a.loc[a[day] == 1, "trip_id"]
        f = t[t["trip_id"].isin(ids)].copy()
        f["date"] = day
        # Groupby trip ID and sort groups by their minimum departure time.
        # For some reason NaN departure times mess up the transform below.
        # So temporarily fill NaN departure times as a workaround.
        f["dt"] = f["departure_time"].fillna(method="ffill")
        f["min_dt"] = f.groupby("trip_id")["dt"].transform(min)
        frames.append(f)

    f = pd.concat(frames)

    return f.drop(["min_dt", "dt"], axis=1)


def create_timetables() -> bool:
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
        metrics_logger.debug(f"creating timetable for route {rt}")

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
            metrics_logger.info(f"{round((rts_count/len(rts)) * 100,3)} complete")

        if not os.path.exists("data/staging/timetables/current_timetables"):
            os.makedirs("data/staging/timetables/current_timetables")

        timetables_df.to_parquet(
            f"data/staging/timetables/current_timetables/rt{rt}_timetable.parquet",
            index=False,
        )

    return True


def dedupe_schedules() -> None:
    """
    given all the historic schedules, dedupe them by date and time by taking only
    scheduled trips from a schedule in which there was not an update

    """
    # get all route in current schedule
    command = """select distinct route_id 
                 from read_parquet('data/staging/timetables/current_timetables/*.parquet')"""

    rts = duckdb.execute(command).df()["route_id"].to_list()
    # testing
    # rts = [21]

    # metric states before
    command = """
            SELECT  COUNT(*) as total_rows,
                    COUNT( DISTINCT cast(bus_stop_time as date)) as total_months,
                    MAX(cast(bus_stop_time as date)) as max_month
            FROM    read_parquet('data/clean_timetables/*.parquet')"""

    stats_before = duckdb.execute(command).df()

    metrics_logger.info(
        f"""Before updating schedule, there were {stats_before['total_rows'].to_list()[0]:,} rows, 
        and {stats_before['total_months'].to_list()[0]:,} unique days. 
        The max month is {stats_before['max_month'].to_list()[0]}."""
    )

    rts_count = 0

    for rt in rts:

        metrics_logger.debug(f"De-duping timetable for route {rt}")

        # find min date of current schedule
        current = pd.read_parquet(
            f"data/staging/timetables/current_timetables/rt{rt}_timetable.parquet"
        )

        current["date_type"] = pd.to_datetime(current["date"])
        min_date = current["date_type"].min()

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

        # if historic exists, then combine
        if os.path.exists(f"data/clean_timetables/rt{rt}_timetable.parquet"):
            # for historic schedule, remove anything never than this date
            historic = pd.read_parquet(
                f"data/clean_timetables/rt{rt}_timetable.parquet"
            )
            historic = historic[historic["bus_stop_time"] < min_date]

            # combine current and historic
            deduped_timetable = pd.concat([current, historic])
        else:
            # historic is not just the current
            deduped_timetable = current

        # convert stop seq to str
        deduped_timetable["stop_sequence"] = deduped_timetable["stop_sequence"].astype(
            str
        )

        deduped_timetable.to_parquet(f"data/clean_timetables/rt{rt}_timetable.parquet")

        if rts_count % 40 == 0:
            metrics_logger.info(f"{round((rts_count/len(rts)) * 100,3)} complete")
        rts_count += 1

    stats_after = duckdb.execute(command).df()

    metrics_logger.info(
        f"""After updating schedule, there were {stats_after['total_rows'].to_list()[0]:,} rows, 
        and {stats_after['total_months'].to_list()[0]:,} unique days. 
        The max month is {stats_after['max_month'].to_list()[0]}."""
    )


def update_schedule() -> None:
    """
    Handle full process to update schedule
        1. Download the current schedule
        2. Create timetables
        3. Dedupe and combine with historic
    """

    # download current schedule
    metrics_logger.info("Downloading current feed")
    download_current_feed()

    # create time table of current schedule
    metrics_logger.info("Creating timetables")
    create_timetables()

    # dedupe with historic schedule
    metrics_logger.info("Deduping timetables")
    dedupe_schedules()


# End -------------------------------------------------------------------------
