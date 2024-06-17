# 0. Imports -------------------------------------------------------------------

# setwd("\\\\wsl.localhost/Ubuntu/home/rmedina/mansueto/cta-stop-watch/cta-stop-watch/acs")

# Clean environment
rm(list = ls())

# Load libraries
library(pacman)
p_load(tidycensus, tidyverse, arrow, sf, sfarrow, mapview, tigris)

# Load census API key from environment
readRenviron("~/.Renviron")
Sys.getenv("CENSUS_API_KEY")

# 1. Load Data -----------------------------------------------------------------

## 1.1. Chicago Data Portal ----------------------------------------------------

### 1.1.1. Bus stops -----------------------------------------------------------

# Load community areas shapefile with stops info
sf_stops <- sf::st_read("../shapefiles/CTA_BusStops/CTA_BusStops.shp")

### 1.1.2. Community areas (77) ------------------------------------------------

sf_communities <- sf::st_read("../shapefiles/Boundaries - Community Areas (current).geojson")

### 1.1.3. Chicago area --------------------------------------------------------

# Chicago city area 
sf_ilcountysub  <- tigris::county_subdivisions(state=17, county=31, cb=TRUE)

sf_chicago      <- sf_ilcountysub   |> 
    filter(NAME == "Chicago")       |> 
    select(region=NAME)

## 1.2. Imputed metrics --------------------------------------------------------

# Ran this one time (avoid it since it overloads my computer)

# df_times_full <- arrow::read_parquet("../analysis/out/analytics_frame.parquet")

# Make a small data set for preprocessing 
# df_times_small <- df_times_full |> 
#     head(1000)
# 
# 
# dim(df_times_full)
# dim(df_times_small)
# 
# # Compute average times by bus stop and route 
# df_avg_time <- df_times_full                         |> 
#     group_by(stpid, pid, p_stp_id, year)            |> 
#     summarise(
#         avg_bus_per_hour = mean(bus_per_hour), 
#         avg_wait_time_minutes = mean(wait_time_minutes)
#     )
# 
# 
# write_parquet(df_avg_time, "../analysis/aggregated_times_stops.parquet")  

df_avg_time <- arrow::read_parquet("../analysis/aggregated_times_stops.parquet")

dim(df_avg_time)

# COUNT STOPS-ROUTES -----------------------------------------------------------

# Paste point geometries 
sf_stops_routes <- df_avg_time |>
    left_join(sf_stops |> mutate(stpid = as.character(SYSTEMSTOP)), 
              by = join_by(stpid)) |> 
    sf::st_as_sf()

dim(sf_stops_routes)

# Point-in-polygon count of stops 
sf_communities$n_stops <- lengths(st_intersects(sf_communities, sf_stops))

mapview(sf_chicago) + 
    mapview(sf_communities, alpha=.05, zcol = "n_stops_patterns")

# Counting combinations of (physical) stops and different patterns
sf_communities$n_stops_patterns <- lengths(st_intersects(sf_communities, sf_stops_routes))

mapview(sf_chicago) + 
    mapview(sf_communities, alpha=.05, zcol = "n_stops_patterns") +
    mapview(sf_stops, alpha = 0, size = 2, cex = 1, col.regions = "grey")



# AGGREGATE WAITING TIMES ------------------------------------------------------

sf_stops_communities <- sf_communities |> 
    st_join(sf_stops_routes)

sf_avg_comms <- sf_stops_communities |> 
    group_by(community, geometry) |> 
    summarise(avg_bus_per_hour = mean(avg_bus_per_hour), 
              avg_wait_time_minutes = mean(avg_wait_time_minutes))


mapview(sf_chicago) + 
    mapview(sf_avg_comms, alpha=.05, zcol = "avg_bus_per_hour") 


mapview(sf_chicago) + 
    mapview(sf_avg_comms, alpha=.05, zcol = "avg_wait_time_minutes") 
