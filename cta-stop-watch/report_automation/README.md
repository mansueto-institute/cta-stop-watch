# Running the Pipeline

This was process was built to be run daily or weekly. To process all of the data see the section (x)

### Recommended Setup
1. Add any raw pattern data already downloaded to `data/patterns_raw/`. Download our archive here.
1. Add all raw_trip data you want to start with (can also only add the most recent one) to `data/raw_trips/`
1. Download any historic processed data and add to `data/processed_by_pid/`. See our archive here.
1. Download a `rt_to_pid.parquet` xwalk and add to `data/`
1. Run `python -m main -s config` to update config file after files have been added or manually update `config.json` file. `utils.create_config()`


### The Daily Process
Run `python -m main -p process`. (`main.process_new_trips()`)

1. `process_trips.update_data(config.MAX_DATE, today)`: Downloads all data from max date of downloaded data to present. Keeps an archive in `data/raw_trips`. Also saves staging data to use for the daily script in `data/staging/days/*`, `data/staging/pids/*` and `data/staging/current_days_download`.parquet`
1. `process_trips.update_patterns()`: Attempts to download and process new patterns that are in the data that are not present. Adds patterns to `data/patterns/raw_patterns/*`. Processes all patterns and adds to `data/patterns/current_patterns`
1. `calculate_stop_time.calculate_patterns(pids)`: Interpolates bus stop times for new trips. Adds files for each pattern to store for the month in `data/staging/trips/{pid}/*`. File format is `trips_{pid}_{pull_date}.parquet`
1. Run `utils.create_config()` to update max date and the list of existing patterns.
1. Update rt to pid xwalk with `util.create_rt_pid_xwalk()`
1. Run `clear_staging(folders=["staging/days", "staging/pids", "raw_trips"],files=["staging/current_days_download.parquet"])` to clear staging files for the next run.


See `process.log` for details of a run

### The Monthly Process

Run `python -m main -p metrics` (`main.process_metrics()`)

1. `update_metrics.combine_recent_trips()`: Merges monthly staged trips in `data/staging/trips/{pid}/*` with historic trips in `data/processed_by_pid/`
1. `metrics_utils.create_rt_pid_xwalk()`: Updates a xwalk of all route and pattern combos for metric creation.
1. `update_schedule.update_schedule()`: Downloads the current schedule to `data/timetables/feed_{date}.zip`. Creates timetables from the schedule in `data/timetables/current_timetables`. Then appends the new schedule to the historic schedules in `data/clean_timetables/*`.
1. `utils.clear_staging(["timetables/current_timetables"])`: Remove current schedules to prepare for subsequent run. 
1. `update_metrics.update_metrics('all')`: Grabs the processed trips from `data/processed_by_pid/` and re calculates metrics. Metrics tables is sent to  `data/metrics/*`
1. Store processed_by_pid to `s3://../processed_by_pid`, clean_timetables to `s3://../clean_timetables`, patterns/patterns_raw to `s3://../patterns_raw`, and staging/timetables/feed_{today}.zip to `s3://../historic_feeds` (need to create this folder) using `store_data.store_all_data()`

See `metrics.log` for details of run


TODO
* metrics add last month


On my computer this weekend to get caught up

1. get processed trips data up to 6/1 in folders
1. get schedule up to 6/1
1. update config to be max date of 2024-05-31
1. run python -m main -p process 
1. run python -m main -p metrics to get updated data

then run storage data
1. all the store_data functions




This weekend on the server can run
1. daily process every day at 1am
1. try monthly process at 3am sunday 
