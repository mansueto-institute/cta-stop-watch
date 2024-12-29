# Mansuteo Institute's StopWatch

## Overview

Part of the [2024 Local Data Journalism Initiative](https://miurban.uchicago.edu/local-data-journalism_2024), this project is a collaboration between the University of Chicago's Mansueto Institute for Urban Innovation and the Chicago Tribune. The StopWatch analyzes a unique dataset of over 100 million real-time bus locations collected by [Chi Hack Night Ghost Bus](https://ghostbuses.com).

The goal of this project is to understand how bus service reliability changed over time in the city of Chicago after the disruptions of the COVID-19 pandemic and across the city’s community areas. To address our research questions, we built a novel, comprehensive data set showing the actual arrival time of each bus at every bus stop in Chicago from June 2022 to July 2024. In addition, we processed schedule data to compare actual performance to planned service. With this data, we computed several metrics to assess the reliability and accessibility of bus service. Our results showed that the CTA initially decreased scheduled service in early 2022 through 2023 to  match the slower real-time performance and later increased it in early 2024. However, these changes were not uniformly felt across Chicago’s community areas. Along with our analysis, we published the bus data sets and created Bus Report Cards — an interactive platform with indicators at the community, route, and bus stop level. These products will be updated through an automated pipeline, which can be consulted in the open-source code repository of this project.

View the project online at https://ctastopwatch.miurban-dashboards.org.

Read the project's report at https://bit.ly/MansuetoStopWatch.

Learn more about the [Mansueto Institute of Urban Innovation](https://miurban.uchicago.edu/).

## Techincal Details

This project contains: 

1. **Scrapping Realtime Bus Locations** - Building upon the work of [Chi Hack Night Ghost Bus project](https://github.com/chihacknight/chn-ghost-buses), the Manseuto Institute has taken over the implementation of maintenance of the data and data pipelines created by Ghost Buses which scrapes real-time bus location data from the CTA bus tracker API get vehicles feed every five minutes. Files of daily locations can be found [here](link). The code for this pipeline can be found in `ghostbus-cta-scrape/`. 

2. **Processed Actual Bus Service Data Set** - Using real time bus location scrapped every 5 minutes from the [CTA bus tracker API](https://www.transitchicago.com/developers/bustracker/), we have created a bus stop level dataset of actual service for the CTA starting June 2022 building off the work of [Chi Hack Night Ghost Bus project](https://github.com/chihacknight/chn-ghost-buses). This pipeline currenly runs daily and the processsed data can be found [here for download](link). The code for this pipeline code can be found in `report_automation/`. See [methods] for more information.

3. **Summary Metrics** - Using both the historic real-time bus location and the historic schedule data bus stop level, we calculated the a set of metrics for different time periods, including hour of the day, day of the week, week of the year, month of the year, year, week for each given year and month for each given year. These summary metrics are used for the Bus Report Cards on the web app. Download the most recently metrics [here](). The code for the metrics creation can be found in `report_automation/`. See [methods] for more information.

4. **Bus Stop Report Cards Web App** - A FastAPI app that includes an interactive Tableau dashboard with indicators at the community, route and bus stop level to allow riders to explore relevant metrics about the stops and routes that they use. The report cards will update monthly with up to date metrics. Access the web app [here](https://ctastopwatch.miurban-dashboards.org/). The code for the web app can be found in `bus_report_cards/`.

5. **Bus Service Analysis** - A first analysis found that the CTA initially decreased scheduled service in early 2022 through 2023 to  match the slower real-time performance and later increased it in early 2024. However, these changes were not uniformly felt across Chicago’s community areas. Notebooks for the analysis can be found in `analysis/`

Lastly, `git-issues-review/` contains exploratory analysis of bugs and improvement for the projects as documented [here](https://github.com/mansueto-institute/cta-stop-watch/issues).

### Methods

The Mansueto StopWatch is centered around the bus stop. We picked this unit of analysis since it constitutes the main point of contact between users and bus service: the stop is where users wait for the bus and it determines how close the service is from their trip origin and destination. Since there is no public data that precisely shows when a bus stopped at a bus stop, our first and major technical challenge was to build such a dataset. This would then be the building block for the aggregated metrics. To produce the dataset, we took a variety of steps to process and merge several data sources, such as community, bus stop and route shapefiles, historic bus location pings, and the historic bus schedule data.

For the breakdown of how we process these inputs, we follow the terminology used by the [CTA API documentation](https://www.transitchicago.com/assets/1/6/cta_Bus_Tracker_API_Developer_Guide_and_Documentation_20160929.pdf):

- **Route**: A collection of patterns  
- **Pattern**: One possible set of stops that a bus can travel on  
- **Trip**: For a given pattern, a bus's journey from the first stop to the last stop  

We began with over 110 million real-time bus locations from June 1, 2022, to July 28, 2024, as provided by the [Chi Hack Night Ghost Bus](https://github.com/chihacknight/chn-ghost-buses) project. These bus locations are real-time data from the [CTA bus tracker API](https://www.transitchicago.com/developers/bustracker/) `get vehicles` feed, which stores real-time data queried every five minutes. Each bus location ping includes metadata such as vehicle number, route, pattern ID, and a trip ID. Due to the lack of uniqueness of the existing trip ID provided, we created a unique trip ID to group a collection of bus locations together. Each trip represents a specific bus on a specific pattern and route at a certain time of the day (e.g., bus with vehicle ID 4654 traveling northbound on pattern 1456 on route 6 on June 30, 2023). While this method is not perfect, this new trip ID allows us to group bus pings into one trip, facilitating analysis of service reliability. Using this method, we identified 10,235,984 unique trips in the original dataset. For our analysis, it was necessary to transform the raw bus location data into the desired bus stop view. The original data represents 5-minute snapshots of every bus in the CTA system, which was converted to the times that each bus passes a bus stop using imputation. For example, if two buses pass a stop within a 5-minute snapshot, there will be two rows, each listing the estimated time the first bus passed a stop and the estimated time since the last bus. This transformation is necessary as it allows us to derive performance metrics that are more interpretable and easier to localize than bus positions.

To do this, we:  

1. Determined the bus stops for a particular trip by using the pattern ID and bus stop locations as provided by the CTA  
2. Combined bus locations and stop locations for a trip spatially  
3. Removed bus locations that were not on route  
4. Interpolated the time a bus arrived at a bus stop by using the time and distance between bus locations and the distance from bus stops between the bus locations  

We then processed historic schedules of bus service to contrast it with the actual service provided. For this purpose, we used [General Transit Feed Specification (GTFS)](https://gtfs.org/) data. The CTA only allows for the download of the current schedule, which was an obstacle considering that we planned to evaluate bus service going back to June 2022. However, [Transit.land](https://www.transit.land/feeds/f-dp3-cta), an open data platform that collects GTFS data, maintains a historic archive of all feeds. Historic feeds back to May 2022 were downloaded. Schedules were recreated from this historic GTFS data using [GTFS Kit](https://github.com/mrcagney/gtfs_kit), an open-source Python library to work with GTFS data.

In addition to bus pings and schedules, the analysis relies on shapefiles of three main units of analysis: community areas, bus stops, and routes. These shapefiles are mainly used for visualizations and for spatial operations. More specifically, we performed point-in-polygon operations to aggregate service performance metrics at the community level—by identifying the bus stops that serve each of the 77 community areas. Up-to-date shapefiles are available at the [Chicago Data Portal](https://data.cityofchicago.org/) for the following spatial units:  

- [Routes](https://data.cityofchicago.org/Transportation/CTA-Bus-Routes-Shapefile/d5bx-dr8z)  
- [Bus stops](https://data.cityofchicago.org/Transportation/CTA-Bus-Stops-Shapefile/pxug-u72f/about_data)  
- [Community areas](https://data.cityofchicago.org/Facilities-Geographic-Boundaries/Boundaries-Community-Areas-current-/cauq-8yn6)  

Using both the historic real-time bus location and the historic schedule data at the bus stop level, we calculated the following metrics for different time periods (including hour of the day, day of the week, week of the year, month of the year, year, week for each given year, and month for each given year).  

#### Metrics include:  

- **Time to next bus stop**  
  - Given a bus is at a bus stop, the time until the next bus on the same route arrives.  

- **Excess Time to Next Bus**  
  - The actual time to next bus minus the scheduled time to next bus.  

- **Trip Duration**  
  - The time difference between the first and last stop.  

- **Trip Delay**  
  - The actual trip duration minus the scheduled trip duration.  

- **Number of buses**  
  - How many buses passed a bus stop in each time interval.  

- **Excess number of buses**  
  - Actual number of buses in each time interval minus scheduled number of buses.  

To calculate the metrics, we:  

1. Filtered to only trips with stops between 6am and 8pm  
2. Calculated the median, mean, standard deviation, max, min, 25th quartile, and 75th quartile for each metric  
3. Aggregated to the route and community area level for varying time periods by finding the weighted median value of the metric for each stop using the number of buses that pass each bus stop in the aggregation unit  

For further details on the project data and methodology, consult the [full report](https://bit.ly/MansuetoStopWatch).

See [project_history.md](project_history.md) for a history of the start of the project.

### Implementation Details

We recommend installing poetry https://python-poetry.org and then running `poetry install` to install the dependencies for the project
