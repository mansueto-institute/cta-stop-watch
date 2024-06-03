from datetime import timedelta, date
import polars as pl
import os
from pathlib import Path
import duckdb

dtype_map = {
    "vid": pl.UInt32,
    "tmstmp": pl.Utf8,
    "lat": pl.Float64,
    "lon": pl.Float64,
    "hdg": pl.UInt32,
    "pid": pl.Float64,
    "rt": pl.Utf8,
    "des": pl.Utf8,
    "pdist": pl.Utf8,
    "dly": pl.Boolean,
    "tatripid": pl.Utf8,
    "origtatripno": pl.Utf8,
    "tablockid": pl.Utf8,
    "zone": pl.Utf8,
    "scrape_file": pl.Utf8,
    "data_time": pl.Utf8,
    "data_hour": pl.Utf8,
    "data_date": pl.Utf8,
}


def get_date_range(start: date, end: date, delta: timedelta):
    cur_date = start
    while cur_date < end:
        yield cur_date
        cur_date += delta


def download_full_day_csv_to_parquet(
    start: date, end: date, delta: timedelta, out_path: Path
):
    URL_HEAD = "https://dmu5hq5f7fk32.cloudfront.net/bus_full_day_data_v2/"
    os.makedirs(out_path, exist_ok=True)
    for day in get_date_range(start, end, delta):
        day_f = day.strftime("%Y-%m-%d")
        day_csv = day_f + ".csv"
        day_parquet = day_f + ".parquet"
        url_day = URL_HEAD + day_csv
        print(url_day)
        df = pl.read_csv(url_day, dtypes=dtype_map)
        df.write_parquet(out_path / day_parquet)


def save_partitioned_parquet(out_file: str):
    cmd_number = f"""COPY
    (SELECT
        *,
        CONCAT(
            rt, pid, origtatripno, tatripid, vid, data_date
        ) AS unique_trip_vehicle_day
    FROM read_parquet('out/parquets/*.parquet'))
    TO '{out_file}'
    (FORMAT 'parquet');"""
    duckdb.execute(cmd_number)


if __name__ == "__main__":
    start = date(year=2023, month=1, day=1)
    end = date(year=2024, month=1, day=1)
    delta = timedelta(days=1)
    folder_path = Path("out/parquets/")
    out_file = "out/2023_cta_bus_full_day_data_v2.parquet"
    download_full_day_csv_to_parquet(start, end, delta, folder_path)
    save_partitioned_parquet(out_file)
