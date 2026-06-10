"""
build_historic_patterns.py

For each GTFS feed zip in archive/scrapers/inp/historic_gtfs/, extract the
feed_start_date from the filename and write per-PID stop/segment parquets
tagged with that date:

    data/patterns/patterns_historic/pid_{pid}_stop_{feed_start_date}.parquet
    data/patterns/patterns_historic/pid_{pid}_segment_{feed_start_date}.parquet

Uses stop_times.txt + stops.txt + trips.txt from each feed to build stop
sequences per pattern (shape_id → pid).

NOTE: build_merged_pattern_data() from process_historic_gtfs.py does an inner
join on exact lat/lon coordinates, which only finds ~4 PIDs in CTA's GTFS.
This script uses stop_times.txt instead, giving all ~800 PIDs per feed.

Idempotent: skips files that already exist.
"""

import sys
import pathlib
import pandas as pd
import polars as pl
from zipfile import ZipFile, BadZipFile

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

HERE = pathlib.Path(__file__).parent
GTFS_ZIP_DIR = (HERE / "../archive/scrapers/inp/historic_gtfs").resolve()
OUT_DIR = HERE / "data/patterns/patterns_historic"
OUT_DIR.mkdir(exist_ok=True)

# Import geometry functions from archive
sys.path.insert(0, str((HERE / "../archive/cta-stop-etl").resolve()))
from add_patterns_from_archive import convert_to_geometries

# ---------------------------------------------------------------------------


def build_pid_stops(zip_path: pathlib.Path) -> pd.DataFrame:
    """
    Extract per-PID stop sequences from a GTFS zip using stop_times.txt.

    Returns a pandas DataFrame with columns:
        pid, seq (stop_sequence), stpid (stop_id), stpnm, lat, lon
    """
    with ZipFile(zip_path) as z:
        with z.open("stops.txt") as f:
            df_stops = pl.read_csv(f, infer_schema_length=0)
        with z.open("trips.txt") as f:
            df_trips = pl.read_csv(f, infer_schema_length=0)
        with z.open("stop_times.txt") as f:
            df_stop_times = pl.read_csv(f, infer_schema_length=0)

    # Derive pid from last 5 chars of shape_id
    df_trips = df_trips.with_columns(
        pl.col("shape_id").str.slice(-5).alias("pid")
    )

    # One representative trip per pid (stops are identical across trips
    # sharing the same shape_id, so any trip will do)
    rep_trips = (
        df_trips.group_by("pid")
        .agg(pl.first("trip_id").alias("trip_id"))
    )

    # Filter stop_times to only representative trips
    rep_trip_ids = rep_trips["trip_id"].to_list()
    df_st = df_stop_times.filter(pl.col("trip_id").is_in(rep_trip_ids))

    # Join stop_times with representative trips to get pid
    df_st = df_st.join(rep_trips, on="trip_id", how="left")

    # Join with stops to get lat/lon/name
    df_stops_slim = (
        df_stops
        .select(["stop_id", "stop_name", "stop_lat", "stop_lon"])
        .rename({"stop_id": "stpid", "stop_name": "stpnm",
                 "stop_lat": "lat",  "stop_lon":  "lon"})
    )
    df_st = df_st.join(df_stops_slim, left_on="stop_id", right_on="stpid", how="left")

    # Keep only needed columns
    df_st = df_st.select([
        "pid", "stop_sequence", "stop_id", "stpnm", "lat", "lon"
    ]).rename({"stop_sequence": "seq", "stop_id": "stpid"})

    # Convert to pandas; cast numeric columns
    df_pd = df_st.to_pandas()
    for col in ("lat", "lon", "seq"):
        df_pd[col] = pd.to_numeric(df_pd[col], errors="coerce")

    # All rows from GTFS stop_times are stops (type S), matching CTA API convention
    df_pd["typ"] = "S"

    return df_pd


def main():
    zips = sorted(GTFS_ZIP_DIR.glob("*.zip"))
    print(f"Found {len(zips)} GTFS feed zips\n")

    for zip_path in zips:
        # Filename: YYYY-MM-DD_YYYY-MM-DD.zip
        # Dates use hyphens, so splitting on underscore gives exactly 2 parts
        feed_start_date = zip_path.stem.split("_")[0]

        print(f"Processing {zip_path.name}  (feed_start_date={feed_start_date})")

        try:
            df_all = build_pid_stops(zip_path)
        except (KeyError, BadZipFile) as e:
            print(f"  Skipping — error reading zip: {e}\n")
            continue

        pids = df_all["pid"].unique()
        print(f"  {len(pids)} PIDs in feed")
        written = skipped = errors = 0

        for pid in pids:
            stop_out = OUT_DIR / f"pid_{pid}_stop_{feed_start_date}.parquet"
            seg_out  = OUT_DIR / f"pid_{pid}_segment_{feed_start_date}.parquet"

            if stop_out.exists() and seg_out.exists():
                skipped += 1
                continue

            df_pid = df_all[df_all["pid"] == pid].copy()

            try:
                df_pattern, df_segment = convert_to_geometries(df_pid)
            except Exception as e:
                print(f"  Warning: PID {pid} geometry failed: {e}")
                errors += 1
                continue

            df_pattern.to_parquet(stop_out)
            df_segment.to_parquet(seg_out)
            written += 1

        print(f"  Written: {written}  Skipped: {skipped}  Errors: {errors}\n")

    print("Done.")


if __name__ == "__main__":
    main()
