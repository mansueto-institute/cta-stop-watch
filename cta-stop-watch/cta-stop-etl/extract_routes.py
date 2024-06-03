import polars as pl


def extract_pid(pid: float):
    df = pl.scan_parquet("out/2023_cta_bus_full_day_data_v2.parquet")
    df_route = df.filter(pl.col("pid") == pid)
    df_route.sink_parquet(f"out/pids/{pid}.parquet")


def extract_list_pids():
    df = pl.scan_parquet("out/2023_cta_bus_full_day_data_v2.parquet")
    df_routes = df.select(pl.col("pid").unique())
    df_routes.collect().write_parquet("out/all_pids_list.parquet")


if __name__ == "__main__":
    extract_list_pids()
    all_pids_df = pl.read_parquet("out/all_pids_list.parquet")
    for row in all_pids_df.iter_rows(named=True):
        print(row["pid"])
        extract_pid(row["pid"])

    # extract_route("6")
    # extract_route("4")
