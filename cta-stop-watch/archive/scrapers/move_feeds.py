"""
Script used for tranferring archived schedules from TransitLand
to repair missing data from server outage from March till August 
of 2025. 
"""

# Libraries 

import os 
import pathlib
import datetime
import subprocess


# Contants 
DATE_FORMAT = "%Y-%m-%d"  
BUCKET_PATH = "gs://miurban-dj-public/cta-stop-watch/historic_gtfs"
DISK_PATH = pathlib.Path(__file__).parent / "inp/historic_gtfs"
MISSING_START_DATE = datetime.datetime.strptime("2025-03-06", DATE_FORMAT)
MISSING_END_DATE = datetime.datetime.strptime("2025-09-08", DATE_FORMAT)

# Functions 

def get_file_date(filename:str):
    start_date = datetime.datetime.strptime(filename[:10], DATE_FORMAT)
    return start_date

def copy_file(file_name, date):
    """

    Args: 
        filename (str): Name of the file of the original TransitLand 
                        archive file with the CTA period 
        date (datetime): Date object for the date for which the 
                        schedule will be copied

    """
    
    # Copy from GCloud instance to storage: https://cloud.google.com/filestore/docs/copying-data
    # https://cloud.google.com/storage/docs/discover-object-storage-gcloud
    command = f"gcloud storage cp {DISK_PATH}/{file_name} {BUCKET_PATH}/feed_{date}.zip"
    subprocess.run(command, shell=True)

if __name__ == "__main__":
    
    historic_feeds = [file for file in os.listdir(DISK_PATH) if file.endswith(".zip")]
    historic_feeds.sort()

    current_date = MISSING_START_DATE
    current_file = historic_feeds.pop(0)
    next_file = historic_feeds.pop(0)
    next_date = get_file_date(next_file)

    delta = datetime.timedelta(days=1)

    while (current_date < MISSING_END_DATE):
        
        if not current_date < next_date: 
            current_file = next_file 
            next_file = historic_feeds.pop(0)
            next_date = get_file_date(next_file)
        
        copy_file(current_file, current_date.date())

        current_date += delta
        
