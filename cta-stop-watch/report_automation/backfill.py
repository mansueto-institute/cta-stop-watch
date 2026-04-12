#!/usr/bin/env python3
"""
backfill.py — Process raw trip data for a date range that was never processed.

Bypasses the normal pipeline's full_download() gate (which skips processing when
no new files are downloaded) and works directly from existing raw_trips/ parquets.

The calculate_pattern() TypeError bug (introduced 2025-09-24, fixed 2025-10-xx)
silently halted all trip processing. This script backfills the gap.

Run from:  cta-stop-watch/report_automation/
Usage:
    python backfill.py                           # 2025-09-24 to yesterday
    python backfill.py --start 2025-10-01        # custom start, yesterday as end
    python backfill.py --start 2025-10-01 --end 2025-12-31
"""

import argparse
from datetime import date, datetime, timedelta
from pathlib import Path

import duckdb
import polars as pl

from calculate_stop_time import calculate_patterns
from utils import process_logger

RAW_PATH = "data/raw_trips"
STAGING_PATH = "data/staging"


def build_current_download(start: date, end: date) -> int:
    """
    Read raw_trips parquets for the given date range and write
    data/staging/current_days_download.parquet (with unique_trip_vehicle_day).

    Returns the number of days successfully staged.
    """
    present = []
    missing = []
    cur = start
    while cur <= end:
        f = Path(f"{RAW_PATH}/{cur}.parquet")
        if f.exists():
            present.append(cur)
        else:
            missing.append(cur)
        cur += timedelta(days=1)

    if not present:
        raise FileNotFoundError(
            f"No raw_trips files found between {start} and {end}. "
            f"Expected files like {RAW_PATH}/YYYY-MM-DD.parquet"
        )
    if missing:
        process_logger.warning(
            f"Missing {len(missing)} raw_trips files: "
            f"{missing[:5]}{'...' if len(missing) > 5 else ''}"
        )

    process_logger.info(
        f"Building current_days_download.parquet from {len(present)} days "
        f"({start} to {end})"
    )

    Path(STAGING_PATH).mkdir(exist_ok=True)

    cmd = f"""COPY
        (SELECT
            *,
            CONCAT(rt, pid, tatripid, vid, data_date) AS unique_trip_vehicle_day
        FROM read_parquet('{RAW_PATH}/*.parquet')
        WHERE data_date >= '{start}' AND data_date <= '{end}')
        TO '{STAGING_PATH}/current_days_download.parquet'
        (FORMAT 'parquet');"""
    duckdb.execute(cmd)

    return len(present)


def extract_pids() -> list[str]:
    """
    Build staging/all_pids_list.parquet and staging/pids/{pid}.parquet
    from the current current_days_download.parquet.

    Returns the list of PID strings.
    """
    df = pl.scan_parquet(f"{STAGING_PATH}/current_days_download.parquet")
    df_pids = df.select(pl.col("pid").cast(pl.Int32, strict=False).unique())
    df_pids.collect().write_parquet(f"{STAGING_PATH}/all_pids_list.parquet")

    all_pids = pl.read_parquet(f"{STAGING_PATH}/all_pids_list.parquet")
    pids = all_pids["pid"].drop_nulls().cast(pl.Utf8).to_list()

    process_logger.info(f"Extracting {len(pids)} PIDs to {STAGING_PATH}/pids/")

    Path(f"{STAGING_PATH}/pids").mkdir(exist_ok=True)
    for i, pid in enumerate(pids):
        if i % 100 == 0:
            process_logger.info(f"  Extracting PID {i}/{len(pids)}...")
        df_route = pl.scan_parquet(f"{STAGING_PATH}/current_days_download.parquet")
        df_route = df_route.filter(
            pl.col("pid").cast(pl.Int32, strict=False) == int(pid)
        )
        df_route.sink_parquet(f"{STAGING_PATH}/pids/{pid}.parquet")

    return pids


def run_backfill(start: str = "2025-09-24", end: str = None) -> None:
    if end is None:
        end = str(date.today() - timedelta(days=1))

    start_dt = datetime.strptime(start, "%Y-%m-%d").date()
    end_dt = datetime.strptime(end, "%Y-%m-%d").date()

    process_logger.info(
        f"\n{'='*60}\nBACKFILL: {start_dt} to {end_dt}\n{'='*60}"
    )

    # Step 1: Build the staging download parquet from raw_trips
    n_days = build_current_download(start_dt, end_dt)
    process_logger.info(f"Step 1 complete: staged {n_days} days")

    # Step 2: Partition into per-PID staging files
    pids = extract_pids()
    process_logger.info(f"Step 2 complete: extracted {len(pids)} PIDs")

    # Step 3: Run the spatial stop-time calculations
    process_logger.info("Step 3: running calculate_patterns() -- this is the slow part")
    calculate_patterns(pids)

    process_logger.info(
        f"\n{'='*60}\n"
        f"BACKFILL COMPLETE\n"
        f"staging/trips/ contains new processed trip files.\n"
        f"\nNext steps:\n"
        f"  1. rsync data/staging/trips/ to server\n"
        f"  2. On server: bash scripts/metrics.sh\n"
        f"{'='*60}"
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Backfill trip processing for a date range."
    )
    parser.add_argument(
        "--start",
        default="2025-09-24",
        help="Start date YYYY-MM-DD (default: 2025-09-24, day after last successful run)",
    )
    parser.add_argument(
        "--end",
        default=None,
        help="End date YYYY-MM-DD inclusive (default: yesterday)",
    )
    args = parser.parse_args()
    run_backfill(args.start, args.end)
