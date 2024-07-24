# Running the Pipeline

This was process was built to be run daily or weekly. To process all of the data see the section (x)

### Recommended Setup
1. Add any raw pattern data already downloaded to data/patterns_raw. Download our archive here.
2. Download any historic processed data and add to data/processed_by_pid
3. Run `python -m main -c` to update config file after files have been added or manually update `config.json` file


### The Daily Process
Run `python -m main -process`. (main.process_new_trips())

1. Downloads all data from max date of downloaded data to present.
2. Attempts to download and process new patterns that are in the data that are not present
3. Interpolates bus stop times for new trips
4. Stores the processed trips in a monthly staging area (data/trips)

See daily.log for details of a run

### The Month

Run `python -m main -metrics` main.

1. Download schedule data from CTA, creates timetables and updates historic schedule
2. Merges monthly staged trips with historic trips
3. Calculates metrics and splits out results in data/metrics



### Outputs
* data/metrics:
    * 
data/patterns