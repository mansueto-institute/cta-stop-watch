import os
import pathlib
import geopandas as gpd
import logging

# Configure a log file to be able to read the logging messages 
logger = logging.getLogger(__name__)

logging.basicConfig(filename='spatial_join.log', 
                    filemode="w",
                    encoding='utf-8', 
                    level=logging.INFO)


# Define functions 
def merge_communities_stops(gdf_areas: gpd.GeoDataFrame, gdf_stops: gpd.GeoDataFrame) -> gpd.GeoDataFrame: 
    
    # Check projection for Chicago
    gdf_areas = gpd.GeoDataFrame(gdf_areas, crs = "EPSG:4326")
    gdf_stops = gpd.GeoDataFrame(gdf_stops, crs = "EPSG:4326")
    gdf_stops["geoms_stops"] = gdf_stops["geometry"]

    # logging.info(f"Column names from AREAS {gdf_areas.columns = }")
    # logging.info(f"Column names from STOPS {gdf_stops.columns = }")
    
    # Spatial join
    gdf_merged = gdf_areas.sjoin(gdf_stops, how="left") 
    
    # logging.info(f"\nMERGED COLUMN NAMES {gdf_merged.columns = }")
    
    # Cast type of stop id
    gdf_merged["stpid"] = gdf_merged["SYSTEMSTOP"].astype("int64").astype("str")

    return gdf_merged


if __name__ == "__main__": 

    SHAPEFILES_DIR = str(pathlib.Path(__file__).parent.parent / "shapefiles/")

    # Load shapefile 
    gdf_communities = gpd.read_file(
        SHAPEFILES_DIR + "/Boundaries - Community Areas (current).geojson")

    # Load bustops 
    gdf_stops = gpd.read_file(
        SHAPEFILES_DIR + "/CTA_BusStops/CTA_BusStops.shp")

    # Spatial join to merge data
    gdf_merged = merge_communities_stops(gdf_communities, gdf_stops)
    
    # Write  
    writing_path = SHAPEFILES_DIR + "/communities_stops.parquet"
    logging.info(f"Writing to path {writing_path}")
    
    print("Writing merged geodata frame")
    gdf_merged.to_parquet(writing_path)

     