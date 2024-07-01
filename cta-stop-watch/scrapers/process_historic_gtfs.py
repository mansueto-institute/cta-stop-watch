import os
import pathlib
from zipfile import ZipFile, BadZipFile
import polars as pl

DIR = pathlib.Path(__file__).parent


def extract_files_from_zip(path: pathlib.Path) -> tuple[pl.DataFrame]:
    """
    Reads all GTFS text files required to build the pattern data with the
    same format as the CTA API and returns them as data frames.

    # Input
        - path: full path of zip folder with all the raw GTFS text files
    # Returns
        - A tuple with the shapes, stops, and trips table from GTFS in
        data frame format.
    """

    with ZipFile(path) as gtfs_zip:
        with gtfs_zip.open("shapes.txt") as shapes:
            df_shapes = pl.read_csv(shapes, infer_schema_length=0)

        with gtfs_zip.open("stops.txt") as stops:
            df_stops = pl.read_csv(stops, infer_schema_length=0)

        with gtfs_zip.open("trips.txt") as trips:
            df_trips = pl.read_csv(trips, infer_schema_length=0)

        with gtfs_zip.open("stop_times.txt") as stop_times:
            df_stop_times = pl.read_csv(stop_times, infer_schema_length=0)

    return df_shapes, df_stops, df_trips, df_stop_times


def prep_shapes(df_shapes: pl.DataFrame) -> pl.DataFrame:
    """
    Takes data frames with GTFS data and reproduces the CTA API "pattern"
    tables in a standardized data frame for further cleaning.

    Input:
        - df_shapes: Data frame with GTFS shapes info
        - df_stops: Data frame with GTFS stops info
        - df_trips: Data frame with GTFS trips info

    Outputs:
        - A data frame with "shape", "stops" and "trip" data in a format
        that resembles the pattern data from the CTA API. A single file is
        produced for all the pids.
    """

    # Preprocess GTFS shapes file to have the same format as the CTA patterns
    df_shapes = df_shapes.rename(
        {
            "shape_pt_lat": "lat",
            "shape_pt_lon": "lon",
            "shape_pt_sequence": "seq",
            "shape_dist_traveled": "pdist",
        }
    )

    # Since the stops data only includes stops points, all other values joined
    # from the shapes data frame must be turns (which the CTA lables as W)
    df_shapes.with_columns(typ=None)

    # Standardize id format with pid
    df_shapes = df_shapes.with_columns(pl.col("shape_id").cast(pl.String))
    df_shapes = df_shapes.with_columns(pl.col("shape_id").str.slice(-5).alias("pid"))

    return df_shapes


def prep_stops(
    df_stops: pl.DataFrame, df_trips: pl.DataFrame, df_stop_times: pl.DataFrame
) -> pl.DataFrame:
    """
    recreate stops for a pattern with geomtery
    """

    # combine trips to get get stopids for a pid
    join1 = df_trips.join(df_stop_times, on="trip_id", coalesce=True)

    stops_pids = join1.select(
        ["route_id", "shape_id", "stop_id", "stop_sequence"]
    ).unique()

    # create pid from shape_id
    stops_pids = stops_pids.with_columns(pl.col("shape_id").cast(pl.String))
    stops_pids = stops_pids.with_columns(pl.col("shape_id").str.slice(-5).alias("pid"))

    stops_pids = stops_pids.drop("shape_id")

    stop_geom = df_stops.select(["stop_id", "stop_lat", "stop_lon"])

    # join geometry for stopid
    stops_pids = stops_pids.join(stop_geom, on="stop_id", how="left", coalesce=True)

    return stops_pids


def main():

    print("\n\nRunning GTFS scraper")

    if not os.path.exists(f"{DIR}/out/gtfs"):
        os.makedirs(f"{DIR}/out/gtfs")

    # Download GTFS data
    # Current data source: https://www.transitchicago.com/downloads/sch_data/
    # https://www.transitchicago.com/downloads/sch_data/google_transit.zip
    print("\n 1. Current GTFS data at CTA website")

    # Load pattern like data frame from must recent GTFS
    current_gtfs_path = DIR / "inp/google_transit.zip"
    df_shapes, df_stops, df_trips, df_stop_times = extract_files_from_zip(
        current_gtfs_path
    )
    df_shapes_processed = prep_shapes(df_shapes)
    df_stops_processed = prep_stops(df_stops, df_trips, df_stop_times)

    # Write output
    print("Writing parquet with gtfs polygons for every pattern (pid).")
    df_shapes_processed.write_parquet("out/gtfs/current_shapes.parquet")
    df_stops_processed.write_parquet("out/gtfs/current_stops.parquet")

    # Historic Data from Transit Land (downloaded zip files from box)
    print("\n 2. Historic GTFS from Transit Land")
    all_zips = os.listdir(f"{DIR}/inp/historic_gtfs")
    folders_to_inspect = []

    if not os.path.exists(f"{DIR}/out/gtfs"):
        os.makedirs(f"{DIR}/out/gtfs")

    for zip_name in all_zips:

        zip_path = DIR / "inp/historic_gtfs" / zip_name

        # Detect if a zip does not contain one of the required text files
        try:
            df_shapes, df_stops, df_trips, df_stop_times = extract_files_from_zip(
                zip_path
            )
        except (KeyError, BadZipFile):
            folders_to_inspect.append(zip_name)
            continue

        df_shapes_processed = prep_shapes(df_shapes)
        df_stops_processed = prep_stops(df_stops, df_trips, df_stop_times)

        print(f"Writing parquet with gtfs from {zip_name}")
        name = zip_name.removesuffix(".zip")

        df_shapes_processed.write_parquet(f"{DIR}/out/gtfs/{name}_segments.parquet")
        df_stops_processed.write_parquet(f"{DIR}/out/gtfs/{name}_stops.parquet")

    print(f"Folders with missing text files:\n\t{folders_to_inspect}")


if __name__ == "__main__":
    main()
