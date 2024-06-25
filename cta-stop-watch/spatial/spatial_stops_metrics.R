# 0. SET UP --------------------------------------------------------------------

# setwd("\\\\wsl.localhost/Ubuntu/home/rmedina/mansueto/cta-stop-watch/cta-stop-watch/acs")

# Clean environment
rm(list = ls())

# Load libraries
library(pacman)
p_load(tidycensus, tidyverse, arrow, sf, sfarrow, mapview, tigris, tmap)

# Load census API key from environment
readRenviron("~/.Renviron")
Sys.getenv("CENSUS_API_KEY")

# Constans
MILE_IN_METERS  <- 1609.34
CHICAGO_BLOCK   <- MILE_IN_METERS / 8

# 1. LOAD SHAPEFILES------------------------------------------------------------

## 1.1. Chicago Data Portal ----------------------------------------------------

### Bus stops ------------------------------------------------------------------

# Load community areas shapefile with stops info
sf_stops <- sf::st_read("../shapefiles/CTA_BusStops/CTA_BusStops.shp")

### Community areas (77) -------------------------------------------------------

sf_communities <- sf::st_read("../shapefiles/Boundaries - Community Areas (current).geojson")

# Compute area in meters
sf_communities$area_m2 <- st_area(sf_communities)

## 1.2. Census tracts ----------------------------------------------------------

# Chicago city area 
sf_ilcountysub  <- tigris::county_subdivisions(state=17, county=31, cb=TRUE)

sf_chicago      <- sf_ilcountysub   |> 
    filter(NAME == "Chicago")       |> 
    select(region=NAME)

# Illinois Census Tracts
sf_il_tracts <- tigris::tracts(state='17', year=2022, cb=TRUE)

# Only tracts at Cook County
sf_cook_tracts <- sf_il_tracts |> filter(COUNTYFP == '031') 

# Only tracts at the city of Chicago 
sf_chicago_tracts <- sf_cook_tracts |> st_intersection(sf_chicago)


## 1.3. Standardize projection -------------------------------------------------

sf_stops            <- st_transform(sf_stops      , crs = "EPSG:4326")
sf_communities      <- st_transform(sf_communities, crs = "EPSG:4326")
sf_ilcountysub      <- st_transform(sf_ilcountysub, crs = "EPSG:4326")
sf_il_tracts        <- st_transform(sf_il_tracts  , crs = "EPSG:4326")

# This data frames had missing CRS
sf_chicago          <- sf_chicago |> st_set_crs("EPSG:4326")
sf_cook_tracts      <- sf_cook_tracts |> st_set_crs("EPSG:4326")
sf_chicago_tracts   <- sf_chicago_tracts |> st_set_crs("EPSG:4326")


# 2. POINT-IN-POLYGON BUS COUNT ------------------------------------------------

## 2.1. Census Tract -----------------------------------------------------------

# st_crs(sf_cook_tracts)$units
# 
# sf_cook_tracts_buffer <- st_buffer(sf_cook_tracts, 1)
# mapview(sf_cook_tracts)

# Count stops by Census Tract 
sf_cook_tracts$n_stops    <- lengths(st_intersects(sf_cook_tracts, sf_stops))
sf_chicago_tracts$n_stops <- lengths(st_intersects(sf_chicago_tracts, sf_stops))

mapview(sf_chicago) + 
    mapview(sf_chicago_tracts, alpha=.05, zcol = "n_stops") +
    mapview(sf_stops, alpha = 0, size = 2, cex = 1, col.regions = "grey")

## 2.2 Community area ----------------------------------------------------------

# Exploratory analysis of community geometries and bus stops 

# Single solid color for community areas and bus stops as bright red dots
mapview(sf_chicago) + 
    mapview(sf_communities, alpha=0.6) +
    mapview(sf_stops, alpha = 0, size = 4, cex = 1, 
            col.regions = "red")

# Create buffers around community areas to include bus stops in the border
sf_communities_buffer <- st_buffer(sf_communities, dist = CHICAGO_BLOCK)

mapview(sf_communities, col.regions = "red") +
    mapview(sf_communities_buffer)

mapview(sf_communities_buffer) +
    mapview(sf_communities, col.regions = "red")

# Count stops by Census Tract 
sf_communities$n_stops_strict <- lengths(st_intersects(sf_communities, sf_stops))
sf_communities$n_stops_buffer <- lengths(st_intersects(sf_communities_buffer, sf_stops))

mapview(sf_chicago) + 
    mapview(sf_communities, alpha=.05, zcol = "n_stops_strict")

mapview(sf_chicago) + 
    mapview(sf_communities, alpha=.05, zcol = "n_stops_buffer") 


# 3. BUS STOP WITHIN WALKING DISTANCE COVERAGE ---------------------------------

# Code trial for defining function 

# TODO: 
    # Check projections are the same
    # Check units of projections
    # Check units of union buffer


# # What is a decent walking distance?
# sf_stops_buffers    <- st_buffer(sf_stops, CHICAGO_BLOCK)
# sf_stops_union      <- st_union(sf_stops_buffers)
# sf_stops_coverage   <- sf_stops_union[2]

# Sanity checks for buffers
# sf::st_is_valid(sf_stops_coverage)
# 
# # Correct mistakes from buffer
# sf_stops_coverage <- sf::st_make_valid(sf_stops_coverage)
# sf::st_is_valid(sf_stops_coverage)
# 
# 
# # mapview(sf_stops_buffers, alpa = 0.2) + 
# #     mapview(sf_stops, col.regions = "red") 
# 
# # Visualize union of buffers 
# tm_shape(sf_communities) + 
#     tm_borders() +
#     tm_shape(sf_stops_coverage) + 
#     tm_fill(col = "blue", alpha = .4) + 
#     tmap_options(check.and.fix = TRUE) +
#     tm_borders(col = "blue")
# 
# # get area coverage 
# sf_stop_intersect <- st_intersection(sf_communities, sf_stops_coverage) 
# 
# sf_stop_coverage_percent <- sf_stop_intersect |> 
#     mutate(intersect_area = st_area(geometry),
#            intersect_area = as.numeric(intersect_area),
#            shape_area = as.numeric(shape_area),
#            area_coverage = intersect_area / shape_area
#            ) |> 
#     st_drop_geometry() |> 
#     dplyr::select(community, contains("area"))
# 
# 
# sf_communities_stops <- sf_communities |> 
#     select(-area_coverage) |> 
#     left_join(sf_stop_coverage_percent |> 
#                   select(community, area_numbe, area_coverage))
# 
# mapview(sf_chicago) + 
#     mapview(sf_communities_stops, alpha=.05, zcol = "area_coverage") 


get_stops_land_coverage <- function(num_blocks = 1){
    # Create buffers for specified number of blocks and correct if mistaken
    sf_stops_buffers    <- st_buffer(sf_stops, CHICAGO_BLOCK * num_blocks)
    sf_stops_coverage   <- st_union(sf_stops_buffers)
    sf_stops_coverage <- sf::st_make_valid(sf_stops_coverage)
    
    if(length(sf_stops_buffers) == 2){
        sf_stops_coverage   <- sf_stops_coverage[2]    
    }
    
    # if(!sf::st_is_valid(sf_stops_coverage)){
    #     # Correct mistakes from buffer
    #     sf_stops_coverage <- sf::st_make_valid(sf_stops_coverage)
    # }
    
    
    # Compute are of coverage 
    sf_stop_intersect <- st_intersection(sf_communities, sf_stops_coverage) 
    
    sf_stop_coverage_percent <- sf_stop_intersect |> 
        mutate(
            intersect_area_m2 = st_area(geometry),
            intersect_area_m2 = as.numeric(intersect_area_m2),
            area_m2 = as.numeric(area_m2),
            area_coverage = intersect_area_m2 / area_m2
        ) |> 
        st_drop_geometry() |> 
        dplyr::select(community, contains("area"))

    
    return(sf_stop_coverage_percent)
    
}

df_coverage_1block <- get_stops_land_coverage(1)
df_coverage_2block <- get_stops_land_coverage(num_blocks = 2)
df_coverage_3block <- get_stops_land_coverage(3)
df_coverage_4block <- get_stops_land_coverage(4)

beepr::beep(4)

sf_communities$percent_1b <- df_coverage_1block$area_coverage
sf_communities$percent_2b <- df_coverage_2block$area_coverage
sf_communities$percent_3b <- df_coverage_3block$area_coverage
sf_communities$percent_4b <- df_coverage_4block$area_coverage


mapview(sf_chicago) + 
    mapview(sf_communities, alpha=.05, zcol = "percent_1b") 

mapview(sf_chicago) + 
    mapview(sf_communities, alpha=.05, zcol = "percent_2b") 

mapview(sf_chicago) + 
    mapview(sf_communities, alpha=.05, zcol = "percent_3b") 

mapview(sf_chicago) + 
    mapview(sf_communities, alpha=.05, zcol = "percent_4b") 


sf_community_stops_metrics <- sf_communities |> 
    select(community, area_numbe, contains("n_"), contains("percent_"), geometry)


st_write_parquet(sf_community_stops_metrics, 
                     "sf_community_stops_metrics.parquet")

# 2. ACS preprocessing ---------------------------------------------------------

## 2.1. Identify ACS variables of interest -------------------------------------

# Explore available variables for ACS
df_vars <- load_variables(2022, "acs5", cache = TRUE)
View(df_vars)

## 1.2. Query ACS --------------------------------------------------------------

# All ACS population variables for every Illinois Census Tract
# df_raw_pop  <- get_acs(
#     geography   = "tract",
#     state       = "IL",
#     table       = "B01001",
#     year        = 2022,
#     cache_table = TRUE
# )
# 
# # glimpse(df_raw_pop)

# Only total population from ACS
sf_tot_pop_tract <- get_acs(
    geography = "tract",
    state     = "IL",
    variables = c(
        total_pop = "B02001_003"
        # medincome = "B19013_001"
    ),
    year      = 2022,
    geometry  = TRUE,
    output    = "wide",
    cache_table = FALSE) |> 
    select(GEOID, pop = total_popE, geometry) |> 
    glimpse()

# 3. MAPS ----------------------------------------------------------------------

## Map Census Tract ------------------------------------------------------------

# Add pop data by census tract 
names(sf_chicago_tracts) 

sf_chicago_tracts <- st_transform(sf_chicago_tracts, crs = "EPSG:4326")
sf_tot_pop_tract <- st_transform(sf_tot_pop_tract, crs = "EPSG:4326")

sf_chi_tracts_stops_acs <- sf_chicago_tracts |> 
    left_join(sf_tot_pop_tract               |> 
                  as.data.frame()            |> 
                  select(-geometry), 
              by = join_by(GEOID))           |>
    mutate(
        stops_percap = n_stops * 100/ pop, 
        stops_percap = if_else(is.infinite(stops_percap), NA_integer_, stops_percap)
        )


summary(sf_chi_tracts_stops_acs$n_stops)
summary(sf_chi_tracts_stops_acs$pop)
summary(sf_chi_tracts_stops_acs$stops_percap)

# Baseline, population by tract
mapview(sf_chicago) + 
    mapview(sf_chi_tracts_stops_acs, alpha=.05, zcol = "pop") +
    mapview(sf_stops, alpha = 0, size = 2, cex = 1, col.regions = "red")



## Map Community Area ----------------------------------------------------------


### Spatial aggregation of pop by tracts to community --------------------------

# Aggregate data at the community level 
sf_tot_pop_tract <- st_transform(sf_tot_pop_tract, crs = "EPSG:4326")

sf_tot_pop_comms <- sf_communities %>% 
    st_join(sf_tot_pop_tract, join = st_intersects, left = TRUE) |> 
    group_by(community, area, area_num_1, comarea_id, geometry, n_stops) |> 
    summarise(pop = sum(pop)) |> 
    mutate(stops_percap = n_stops * 1000/ pop)

# Quality Checks 
sum(sf_tot_pop_tract$pop) # 1,774,605
sum(sf_tot_pop_comms$pop) # 1,714,806 # Just communities in Chicago

nrow(sf_stops) # All stops 
sum(sf_communities$n_stops) # Stops in Chicago communities
sum(sf_tot_pop_comms$n_stops) # When spatially aggregated from tracts

# glimpse(df_tot_pop)
View(sf_tot_pop_tract)
names(sf_tot_pop_tract)


mapview(sf_chicago) + 
    mapview(sf_tot_pop_comms, alpha=.05, zcol = "pop") 

mapview(sf_chicago) + 
    mapview(sf_communities, alpha=.05, zcol = "n_stops")  +
    mapview(sf_stops, alpha = 0, size = 1, cex = 1, col.regions = "grey")

mapview(sf_chicago) + 
    mapview(sf_tot_pop_comms, alpha=.05, zcol = "stops_percap") 


# 4. Time indicators -----------------------------------------------------------

# Load time indicators
df_avg_time <- arrow::read_parquet("../analysis/aggregated_times_stops.parquet")

# Paste point geometries 
sf_stops_routes <- df_avg_time |>
    left_join(sf_stops |> mutate(stpid = as.character(SYSTEMSTOP)), 
              by = join_by(stpid)) |> 
    sf::st_as_sf()

sf_communities$n_stops_route <- lengths(st_intersects(sf_communities, sf_stops_routes))

# Buses per hour 
sf_stops_communities <- sf_communities |> 
    st_join(sf_stops_routes)

sf_avg_comms <- sf_stops_communities |> 
    group_by(community, geometry)       |> 
    summarise(avg_bus_per_hour = sum(avg_bus_per_hour), 
              avg_wait_time_minutes = mean(avg_wait_time_minutes))

## PRESENTATION ----------------------------------------------------------------


mapview(sf_chicago) + 
    mapview(sf_communities, alpha=0.8, zcol = "community", legend = FALSE) 

mapview(sf_chicago) + 
    mapview(sf_communities, alpha=0.8, zcol = "community", legend = FALSE) +
    mapview(sf_stops, alpha = 0.2, size = 4, cex = 2, 
            col.regions = "red")

mapview(sf_chicago) + 
    mapview(sf_communities, alpha=.05, zcol = "n_stops") +
    mapview(sf_stops, alpha = 0, size = 2, cex = 1, 
            col.regions = "grey")

# mapview(sf_chicago) + 
#     mapview(sf_tot_pop_comms, alpha=.05, zcol = "stops_percap") 

mapview(sf_chicago) + 
    mapview(sf_communities, alpha=.05, zcol = "n_stops_route") 

mapview(sf_chicago) + 
    mapview(sf_avg_comms, alpha=.05, zcol = "avg_bus_per_hour") 



# # 2. Stops data + ACS --------------------------------------------------------
# 
# # 2.1. Merge total population with community areas -----------------------------
# 
# # Check geometries are correct
# plot(sf::st_geometry(df_stops))
# 
# # Apply CRS for Chicago
# st_crs(df_stops) = "EPSG:4326"
# st_crs(df_tot_pop) = "EPSG:4326"
# 
# # Perform spatial join
# sf::st_join(df_tot_pop, df_stops)
# 
# sf::st_within(df_tot_pop, df_stops)
# 
# 
# 
# # 3. Tracts and stops ----------------------------------------------------------
# 
# # Illinois Census Tracts
# sf_il_tracts <- tigris::tracts(state='17', year=2022, cb=TRUE)
# st_crs(sf_il_tracts) = "EPSG:4326"
# 
# # Join tracts with ACS population data 
# sf_cook_tracts_acs <- sf_il_tracts |> 
#     st_join(
#         df_tot_pop |> select(GEOID, total_popE), 
#         join = st_within
#     ) |> 
#     # Only tracts at Cook County
#     filter(COUNTYFP == '031') 
# 
# # Chicago city area 
# sf_ilcountysub  <- tigris::county_subdivisions(state=17, county=31, cb=TRUE)
# st_crs(sf_ilcountysub) = "EPSG:4326"
# 
# sf_chicago      <- sf_ilcountysub   |> 
#     filter(NAME == "Chicago")       |> 
#     select(region=NAME)
# 
# st_crs(sf_cook_tracts_acs) = "EPSF:4326"
# st_crs(sf_chicago) = "EPSF:4326"
# 
# # Only tracts at the city of Chicago 
# sf_chicago_tracts_acs <- sf_cook_tracts_acs |> 
#     st_intersection(sf_chicago)
# 
# mapview(sf_chicago) + 
#     mapview(sf_chicago_tracts_acs, alpha=.05, col.regions='blue')
# 
# 
# sf_stops <- st_as_sf(df_stops |> 
#                          select(stpid, geoms_stops), 
#                      coords = geoms_stops)
# 
# st_crs(sf_stops) = "EPSF:4326"
# 
# # Maps -------------------------------------------------------------------------
# mapview(sf_stops$geoms_stops)
# 
# # END. -------------------------------------------------------------------------