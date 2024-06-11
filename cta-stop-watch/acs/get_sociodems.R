# 0. Imports -------------------------------------------------------------------

install.packages("sfarrow")

# Load libraries
library(tidycensus)
library(tidyverse)
library(arrow)
library(sf)
library(sfarrow)

# Load census API key from environment
readRenviron("~/.Renviron")
Sys.getenv("CENSUS_API_KEY")


# 1. ACS preprocessing ---------------------------------------------------------

## 1.1. Identify ACS variables of interest -------------------------------------

# Explore available variables for ACS
df_vars <- load_variables(2022, "acs5", cache = TRUE)
View(df_vars)

## 1.2. Query ACS --------------------------------------------------------------

# All ACS population variables for every Illinois Census Tract
df_raw_pop  <- get_acs(
    geography = "tract", 
    state = "IL",
    table = "B01001",
    year = 2020, 
    cache_table = TRUE
)

# glimpse(df_raw_pop)


# Only total population from ACS
df_tot_pop <- get_acs(
    geography = "tract", 
    state = "IL",
    variables = c(
        total_pop = "B01001_003"
    ),
    year = 2020, 
    geometry = TRUE, 
    cache_table = TRUE
)

# glimpse(df_tot_pop)
View(df_tot_pop)

# 2. Stops data + ACS ----------------------------------------------------------

# 2.1. Merge total population with community areas -----------------------------

# Load community areas shapefile with stops info
df_stops <- st_read_parquet("../shapefiles/communities_stops.parquet")
# str(df_stops)

# Check geometries are correct
plot(sf::st_geometry(df_stops))

# Apply CRS for Chicago
st_crs(df_stops) = "EPSG:4326"
st_crs(df_tot_pop) = "EPSG:4326"

# Perform spatial join
sf::st_join(df_tot_pop, df_stops)

sf::st_within(df_tot_pop, df_stops)

# END. -------------------------------------------------------------------------