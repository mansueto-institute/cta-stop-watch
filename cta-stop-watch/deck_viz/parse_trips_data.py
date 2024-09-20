import os 
import pathlib 
import pandas as pd
import geopandas as gdp 
import json
import logging


# Configure a log file to be able to read the logging messages ----------------
logger = logging.getLogger(__name__)

logging.basicConfig(filename='parsing_trips_data.log', 
                    filemode="w",
                    encoding='utf-8', 
                    level=logging.INFO)

# Define functions ------------------------------------------------------------

def parse_for_js(trips_data: pd.DataFrame) -> list[dict]: 
    json_data = []

    for _, point in trips_data.iterrows(): 
        point_dict = {}
        point_dict["position"] = [point["lat"], point["lon"]]
        point_dict["id"] = point["vid"]
        point_dict["tmstmp"] = point["data_time"]
        point_dict["delayed"] = point["dly"]

        json_data.append(point_dict)

    return json_data


# Execute code from main file -------------------------------------------------

if __name__ == "__main__": 
    
    # Locate folder with trips data by day 
    TRIPS_PATH = pathlib.Path(__file__).parent.parent / "cta-stop-etl/out/parquets/"
    all_dates = os.listdir(TRIPS_PATH)
    
    # Import an example parquet for a single day of trips 
    file_path = TRIPS_PATH  / str(all_dates[0])
    logging.info(f"Original location of parquet file {file_path}")
    
    trips_data = pd.read_parquet(file_path)

    json_data = parse_for_js(trips_data)

    print(f"Writing json file for {all_dates[0]}")
    
    with open('trip_data.json', 'w', encoding='utf-8') as f:
        json.dump(json_data, f, ensure_ascii=False, indent=4)

# END. ------------------------------------------------------------------------