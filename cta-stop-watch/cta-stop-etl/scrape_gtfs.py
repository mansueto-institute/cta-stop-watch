import polars as pl
import pathlib


def build_merged_shape_data(gtfs_path: str) -> pl.DataFrame:
    """
    Loads GTFS's "shape" data and returns a standardized data frame for further
    cleaning.

    Input:
        - gtfs_path (str): Path were the gtfs files are stored

    Outputs:
        - A data frame with "shape", "stops" and "trip" data in a format
        that resembles the pattern data from the CTA API. A single file is
        produced for all the pids.
    """
    file_name = gtfs_path + "shapes.txt"
    file_path = pathlib.Path(__file__).parent / file_name

    # Load shapes an rename columns
    df_shapes = pl.read_csv(file_path, infer_schema_length=0)

    df_shapes = df_shapes.rename(
        {
            "shape_pt_lat": "lat",
            "shape_pt_lon": "lon",
            "shape_pt_sequence": "seq",
            "shape_dist_traveled": "pdist",
        }
    )

    # Load stops
    file_name = gtfs_path + "stops.txt"
    file_path = pathlib.Path(__file__).parent / file_name

    df_stops = pl.read_csv(file_path, infer_schema_length=0)

    df_stops = df_stops[["stop_id", "stop_name", "stop_lat", "stop_lon"]]
    df_stops = df_stops.rename(
        {"stop_id": "stpid", "stop_name": "stpnm", "stop_lat": "lat", "stop_lon": "lon"}
    )

    df_stops = df_stops.with_columns(pl.lit("S").alias("typ"))

    # Merge
    df_patterns = df_shapes.join(df_stops, on=["lat", "lon"])

    # Change values conditionally
    df_patterns.with_columns(
        (pl.when(pl.col("typ") == "S").then(pl.lit("S")).otherwise(pl.lit("W"))).alias(
            "typ"
        )
    )

    # Add ids from trips file
    file_name = gtfs_path + "trips.txt"
    file_path = pathlib.Path(__file__).parent / file_name

    df_trips = pl.read_csv(file_path, infer_schema_length=0)

    df_patterns = df_patterns.join(df_trips, on=["shape_id"])

    # Standardize id format with pid
    df_patterns = df_patterns.with_columns(pl.col("shape_id").cast(pl.String))
    df_patterns = df_patterns.with_columns(
        pl.col("shape_id").str.slice(-4).alias("pid")
    )

    # Write output
    print("Writing parquet with gtfs polygons for every pattern (pid).")
    df_patterns.write_parquet("out/gtfs/current_shapes.parquet")

    return True


# def import_historic_gtfs():
#     pass

# def convert_to_geometries(df_patterns: pl.DataFrame):
#     pass


if __name__ == "__main__":

    print("Running GTFS scraper")

    # Download GTFS data
    # Current data source: https://www.transitchicago.com/downloads/sch_data/
    # https://www.transitchicago.com/downloads/sch_data/google_transit.zip

    # Load pattern like data frame from must recent GTFS
    gtfs_path = str(pathlib.Path(__file__).parent) + "/cta_current_GTFS/"

    build_merged_shape_data(gtfs_path)
