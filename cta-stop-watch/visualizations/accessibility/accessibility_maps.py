# TODO 
    # Make implementation for matplotlib
    # Plot all stops within a community 


# IMPORTS ---------------------------------------------------------------------

import os 
import polars as pl 
import geopandas as gpd
import pathlib
import logging
import time
import folium 
from selenium import webdriver
from selenium.webdriver.remote.remote_connection import LOGGER
import seaborn as sns
from matplotlib import pyplot as plt
import contextily as cx

# LOGGER ----------------------------------------------------------------------

# Set selenium logger to warning level
LOGGER.setLevel(logging.WARNING)


# Start logger for this script
logger = logging.getLogger(__name__)

logging.basicConfig(
    filename="maps.log",
    filemode="w",
    # format = "%(levelname)-8s [%(filename)s:%(lineno)d] %(message)s",
    encoding="utf-8",
    level=logging.DEBUG,
    # level=logging.INFO
)

# formatter = logging.Formatter('[%(asctime)s] p%(process)s {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s','%m-%d %H:%M:%S')

start_tmstmp = time.time()
start_string = time.asctime(time.localtime())
logging.info(f"SCRIPT STARTED: {start_string}")


# CONTANTS --------------------------------------------------------------------

# Paths 
DIR = pathlib.Path(__file__) 
DIR_INP = DIR.parents[2] / "cta-stop-etl/out/"
DIR_PID = DIR.parents[2] / "cta-stop-etl/out/"
DIR_SHAPES = DIR.parents[2] / "shapefiles/"
DIR_MAPS = ""
# Plots colors 
v_colors = list(sns.color_palette("OrRd").as_hex())

# Data 
# GDF_STOP_ACCESSIBILITY_SHAPES = gpd.read_parquet("stop_access_shapes.parquet")
GDF_STOP_ACCESSIBILITY_SHAPES = gpd.read_parquet("stop_access_shapes_bridgeport.parquet")


# FUNCTIONS -------------------------------------------------------------------

def get_all_communities() -> list[str]: 
    """
    Takes the name or id of a community and returns the list of bus stops 
    from that community. 
    """
    df_communities = pl.read_parquet(f"{DIR_SHAPES}/communities_stops.parquet")
    community_names = list(df_communities["community"].unique())

    return community_names



def plot_one_path_with_folium(gdf: gpd.GeoDataFrame, stop_id: str): 
    """
    Takes spatial data of all shapes and the stop to map 
    This uses folium and produces an interactive map
    It converts it to static maps by taking screenshots with firefox driver
    """   

    gdf_stop = gdf[gdf["origin_stop"] == stop_id]

    # Reorder layers (shortest travel times on top)
    gdf_stop = gdf_stop.sort_values("minutes")
    gdf_stop = gdf_stop.iloc[::-1] # Decreasing order

    
    # Drop time delta type of columns for folium to work 
    gdf_stop = gdf_stop.drop(columns = ["time_budget", "minutes"])

    # Generate map with paths
    map_viz = gdf_stop.explore(column = "time_label", 
                                cmap = "OrRd",
                                )


    # Add bus stop point 
    stop_point = gdf_stop["ls_geometry"].unique()[0]
    map_viz.add_child(
        folium.Circle(location = [stop_point.y, stop_point.x], 
                    radius = 40, 
                    color = "white", 
                    opacity = 1,
                    fill = True, 
                    colorFill = "white", 
                    fill_opacity = 1)
                    )

    map_file = "map.html"
    map_viz.save(map_file)
    
    map_url = 'file://{0}/{1}'.format(os.getcwd(), map_file)
    
    driver = webdriver.Firefox()
    driver.get(map_url)
    time.sleep(5)
    driver.save_screenshot(f"maps/map_stop_{stop_id}.png")
    driver.quit()



def plot_one_path_with_matplotlib(gdf: gpd.GeoDataFrame, stop_id: str):    

    gdf_stop = gdf[gdf["origin_stop"] == stop_id]

    # Reorder layers (shortest travel times on top)
    gdf_stop = gdf_stop.sort_values("minutes")
    gdf_stop = gdf_stop.iloc[::-1] # Decreasing order

    
    # Drop time delta type of columns for folium to work 
    gdf_stop = gdf_stop.drop(columns = ["time_budget", "minutes"])

    # Generate map with paths
    gdf_stop.plot(column = "time_label", 
                            cmap = "OrRd",
                                )


    # Add bus stop point 

    # Save plot 
    plt.savefig(f"{stop_id}.png")



def find_community_stops(community_name: str) -> list[int]: 
    """
    Takes the name or id of a community and returns the list of bus stops 
    from that community. 
    """
    df_communities = pl.read_parquet(f"{DIR_SHAPES}/communities_stops.parquet")
    
    stops = df_communities.with_columns(
        stpid = pl.col("stpid").cast(pl.String)
    ).filter(pl.col("community") == community_name)["stpid"].to_list()

    return stops

def plot_all_community_stops(community_name: str): 
    logging.info(f"{'-'*68}")
    logging.info(f"{community_name}\n")
    
    # Load data 
    gdf_boundaries = gpd.read_file("../../shapefiles/Boundaries - Community Areas (current).geojson")
    gdf_time = gpd.read_parquet(f"out/communities/{community_name}_stops_discrete.parquet")

    # Filter and preprocess for plot
    gdf_shape_community = gdf_boundaries[gdf_boundaries["community"] == community_name]
    gdf_time = gdf_time.drop(columns = ["time_budget"]).sort_values(by = "minutes", ascending = False)

    # Base map
    m = folium.Map(
        location=[41.83491987636846, -87.62004994799588],
        zoom_start=11,
        zoomSnap=0.5,
        min_zoom=10,
        max_zoom=12,
        min_lat=41.6,
        max_lat=42.1,
        min_lon=-87.27481026390364,
        max_lon=-87.981026390364,
        max_bounds=True,
        tiles = "CartoDB positron"
    )

    m = gdf_shape_community.explore(m = m, 
                                    name="Boundaries", 
                                    tiles="CartoDB positron")

    m = gdf_time.explore(m = m,
                    name = "Paths", 
                    column = "minutes", 
                    cmap = "summer",
                    # cmap = "RdYlGn_r",
                    opacity = 0.1,
             )

    # PLOT WITH MATPLOTLIB
    map_file = "map.html"
    m.save(map_file)
    
    map_url = 'file://{0}/{1}'.format(os.getcwd(), map_file)
    
    driver = webdriver.Firefox()
    driver.get(map_url)
    time.sleep(5)
    driver.save_screenshot(f"maps/communities/{community_name}_folium.png")
    driver.quit()


if __name__ == "__main__":

    # gdf = GDF_STOP_ACCESSIBILITY_SHAPES
    # gdf["time_budget"] = gdf["time_budget"].astype(str)

    # All stops 
    # every_chicago_stop = list(gdf["origin_stop"].unique())

    # for stop_id in every_chicago_stop: 
    
    #     if os.path.exists(f"maps/map_stop_{stop_id}.png"):
    #         continue
        
    #     # Plot and store one stop 
    #     plot_one_path_with_folium(gdf_stop_areas, stop_id = stop_id)


    all_communities = get_all_communities()

    for community in all_communities: 
        try:
            plot_all_community_stops(community_name = community)
        except FileNotFoundError: 
            logging.error(f"No data for: {community}\n")


    # Notify finished
    os.system('spd-say "your program has finished"')
