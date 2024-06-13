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

# 1. Load Shapefiles -----------------------------------------------------------

## 1.1. Chicago Data Portal ----------------------------------------------------

### 1.1.1. Bus stops -----------------------------------------------------------

# Load community areas shapefile with stops info
sf_stops <- sf::st_read("../shapefiles/CTA_BusStops/CTA_BusStops.shp")

### 1.1.2. Community areas (77) ------------------------------------------------

sf_communities <- sf::st_read("../shapefiles/Boundaries - Community Areas (current).geojson")

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


# 1.3. Standardize projection --------------------------------------------------

sf_stops            <- st_transform(sf_stops      , crs = "EPSG:4326")
sf_communities      <- st_transform(sf_communities, crs = "EPSG:4326")
sf_ilcountysub      <- st_transform(sf_ilcountysub, crs = "EPSG:4326")
sf_il_tracts        <- st_transform(sf_il_tracts  , crs = "EPSG:4326")

# This data frames had missing CRS
sf_chicago          <- sf_chicago |> st_set_crs("EPSG:4326")
sf_cook_tracts      <- sf_cook_tracts |> st_set_crs("EPSG:4326")
sf_chicago_tracts   <- sf_chicago_tracts |> st_set_crs("EPSG:4326")


# 2. Spatial join for stops ----------------------------------------------------

## STOPS BY CENSUS TRACT -------------------------------------------------------

# Join stops
sf_stops_tracts <- sf::st_join(sf_cook_tracts, sf_stops, 
                               join = st_intersects, 
                               left = TRUE)

# Count stops by Census Tract 
sf_cook_tracts$n_stops    <- lengths(st_intersects(sf_cook_tracts, sf_stops))
sf_chicago_tracts$n_stops <- lengths(st_intersects(sf_chicago_tracts, sf_stops))

mapview(sf_chicago) + 
    mapview(sf_chicago_tracts, alpha=.05, zcol = "n_stops") +
    mapview(sf_stops, alpha = 0, size = 2, cex = 1, col.regions = "grey")

## STOPS BY COMMUNITY AREA -----------------------------------------------------

# Count stops by Census Tract 
sf_communities$n_stops <- lengths(st_intersects(sf_communities, sf_stops))

mapview(sf_chicago) + 
    mapview(sf_communities, alpha=.05, zcol = "n_stops") +
    mapview(sf_stops, alpha = 0, size = 2, cex = 1, 
            col.regions = "grey")


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

# CENSUS TRACT -----------------------------------------------------------------

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



# COMMUNITY --------------------------------------------------------------------


## SPATIAL JOIN -------------

# Aggregate data at the community level 
sf_tot_pop_tract <- st_transform(sf_tot_pop_tract, crs = "EPSG:4326")

sf_tot_pop_comms <- sf_communities %>% 
    st_join(sf_tot_pop_tract, join = st_intersects, left = TRUE) |> 
    group_by(community, area, area_num_1, comarea_id, geometry, n_stops) |> 
    summarise(pop = sum(pop)) |> 
    mutate(stops_percap = n_stops * 1000/ pop)

# Quality Checks 
sum(sf_tot_pop_tract$pop) # 1,774,605
sum(sf_tot_pop_comms$pop) # 1,714,806 # Just communitites in Chicago

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