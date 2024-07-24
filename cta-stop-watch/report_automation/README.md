# Running the Pipeline

This was process was built to be run daily or weekly. To process all of the data see the section (x)

### Recommended Setup
1. Add any raw pattern data already downloaded to `data/patterns_raw/`. Download our archive here.
2. Add all raw_trip data you want to start with (can also only add the most recent one) to `data/raw_trips/`
2. Download any historic processed data and add to `data/processed_by_pid/`. See our archive here.
3. Run `python -m main -s config` to update config file after files have been added or manually update `config.json` file. `utils.create_config()`


### The Daily Process
Run `python -m main -p process`. (`main.process_new_trips()`)

1. `process_trips.update_data(config.MAX_DATE, today)`: Downloads all data from max date of downloaded data to present. Keeps an archive in `data/raw_trips`. Also saves staging data to use for the daily script in `data/staging/days/*`, `data/staging/pids/*` and `data/staging/current_days_download`.parquet`
2. `process_trips.update_patterns()`: Attempts to download and process new patterns that are in the data that are not present. Adds patterns to `data/patterns/raw_patterns/*`. Processes patterns and adds to `data/patterns/current_patterns`
3. `calculate_stop_time.calculate_patterns(pids)`: Interpolates bus stop times for new trips. Adds files for each pattern to store for the month in `data/staging/trips/{pid}/*`. File format is `trips_{pid}_{pull_date}.parquet`
4. Run `utils.create_config()` to update max date and the list of existing patterns.
5. Run `utils.clear_staging(folders=["days", "pids"], files=["current_days_download.parquet"])` to clear staging files for the next run.
6. push to s3

See `process.log` for details of a run

### The Monthly Process

Run `python -m main -p metrics` (`main.process_metrics()`)

1. `update_metrics.combine_recent_trips()`: Merges monthly staged trips in `data/staging/trips/{pid}/*` with historic trips in `data/processed_by_pid/`
2. `metrics_utils.create_rt_pid_xwalk()`: Updates a xwalk of all route and pattern combos for metric creation.
3. `update_schedule.update_schedule()`: Downloads the current schedule to `data/timetables/feed_{date}.zip`. Creates timetables from the schedule in `data/timetables/current_timetables`. Then appends the new schedule to the historic schedules in `data/clean_timetables/*`.
4. `utils.clear_staging(["timetables/current_timetables"])`: Remove current schedules to prepare for subsequent run. 
5. `update_metrics.update_metrics('all')`: Grabs the processed trips from `data/processed_by_pid/` and re calculates metrics. Metrics tables is sent to  `data/metrics/*`
6. push to s3

See `metrics.log` for details of run


TODO
* metrics add last month
* add log statements for metrics run
* full run of each
* time is tomorrow, today, yesterday, a week ago


