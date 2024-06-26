import polars as pl


def extract_pid(pid: int):
    df = pl.scan_parquet("out/cta_bus_full_day_data_v2.parquet")
    df_route = df.filter(pl.col("pid").cast(pl.Int32, strict=False) == pid)
    df_route.sink_parquet(f"out/pids/{pid}.parquet")


def extract_list_pids():
    df = pl.scan_parquet("out/cta_bus_full_day_data_v2.parquet")
    df_routes = df.select(pl.col("pid").cast(pl.Int32, strict=False).unique())
    df_routes.collect().write_parquet("out/all_pids_list.parquet")


def extract_routes():
    extract_list_pids()
    all_pids_df = pl.read_parquet("out/all_pids_list.parquet")
    for row in all_pids_df.iter_rows(named=True):
        print(row["pid"])
        extract_pid(row["pid"])
