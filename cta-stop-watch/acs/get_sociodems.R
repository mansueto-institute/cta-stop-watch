# 0. Imports -------------------------------------------------------------------


# Load libraries
library(pacman)
p_load(tidycensus, tidyverse, arrow, sf, sfarrow, mapview, tigris)

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
        total_pop = "B02001_001", 
        medincome = "B19013_001"
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



# 3. Tracts and stops ----------------------------------------------------------

sf_il_tracts <- tigris::tracts(state='17', year=2021, cb=TRUE)

sf_il_tracts

sf_cook_tracts_acs <- sf_il_tracts %>% 
    left_join(
        df_tot_pop %>% 
            select(GEOID, contains('Share'), total_pop, medincome)
    ) %>% 
    filter(COUNTYFP == '031') #cook


# Maps -------------------------------------------------------------------------

il21 <- get_acs(geography = "tract", 
                variables = c(medincome = "B19013_001",
                              totalpop = "B02001_001",
                              white_alone = "B02001_002",
                              black_alone = "B02001_003",
                              asian_alone = "B02001_004"),
                state = "IL", 
                year = 2021,
                output='wide')

il21 <- il21 %>% 
    mutate(
    `White Share` = white_aloneE / totalpopE,
    `Black Share` = black_aloneE / totalpopE,
    `Other Share` = (totalpopE - white_aloneE - black_aloneE) / totalpopE
)

iltracts <- tigris::tracts(state='17', year=2021, cb=TRUE)


cook_tracts_acs <- iltracts %>% 
    left_join(
    il21 %>% 
        select(GEOID, contains('Share'), totalpopE, medincomeE)
    ) %>% 
    filter(COUNTYFP == '031') #cook

ilcountysub <- tigris::county_subdivisions(state=17, county=31, cb=TRUE)
chicago <- ilcountysub %>% filter(NAME == "Chicago") %>% select(region=NAME)

chicago_tracts_acs <- cook_tracts_acs %>% st_intersection(
    chicago
)

mapview(chicago) + 
    mapview(chicago_tracts_acs, alpha=.05, col.regions='blue')

# END. -------------------------------------------------------------------------