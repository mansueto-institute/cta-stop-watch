from cta-stop-etl.create_data_part import full_download
from cta-stop-etl.calculate_stop_time import full_download
from db_utils import get_max_date, add_patterns_to_db
from scrapers import query_cta_api
from datetime import date

# 1
    # check max date of data in the database = max_date
    # download data from ghost buses from max_date to today

def update_data():
    max_date = get_max_date()
    today = date.today()

    
    full_download(max_date, today)

# 2 
    # check if there are new patterns in the new data

def update_patterns():
    pass
    # get all patterns in the database from new data
    new_trip_pids

    # get all processed patterns
    processed_pids

    # compare the two if there are new patterns, download from api and add them to the database
    new_patterns = list(set(new_trip_pids) - set(processed_pids))

    if len(new_patterns) > 0:
        # query_cta_api(pid)
        patterns = download_patterns(new_patterns)

        add_patterns_to_db(patterns)

# 3
    # interpolate for new trips
    calculate_patterns(new_trip_pids)
    # update db
    add_trips_db()

# 4 
    #recompute metrics
def update_metrics():
    pass
    # grab data from db and compute metric
    compute_metrics()
    # update db metrics table
    add_metrics_db()



def main():
    update_data()
    update_patterns()
    update_metrics()


