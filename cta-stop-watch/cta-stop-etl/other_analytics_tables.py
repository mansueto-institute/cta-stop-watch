from geopandas import GeoDataFrame, GeoSeries
import pandas as pd
import geopandas as gpd


def identify_detour_pings(trip: GeoDataFrame, segments_df: GeoDataFrame):
    """
    identify which pings are not on the router

    returns df of trip with added boolean flag
    """

    trip["id1"] = trip["id"]

    on_route_df = trip.sjoin(segments_df, how="inner", predicate="within")
    on_route_df["id2"] = on_route_df["id1"]

    final_df = trip.merge(
        on_route_df,
        how="left",
        left_on="id1",
        right_on="id2",
    )

    final_df["is_on_route"] = final_df["id2"].notnull()

    final_df = final_df.drop(columns=["id1", "id2"])

    return final_df
