from trip_processing import process_new_trips
from update_metrics import update_metrics
from update_schedule import update_schedule


# every day do
process_new_trips()

# once a month do
update_metrics("all")
update_schedule()
