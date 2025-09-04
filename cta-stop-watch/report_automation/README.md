# Running the Pipeline

A pipeline to continuously process ghost bus data. Trips are currently being processed daily and metrics are recalculated at the beginning of the month.

um### Recommended Setup
1. [Clone repo](https://docs.github.com/en/repositories/creating-and-managing-repositories/cloning-a-repository)
1. [Install uv](https://docs.astral.sh/uv/getting-started/installation/)
1. `cd` into repo and run `uv sync`
1. `cd` to `cta-stop-watch/cta-stop-watch/report_automation`
1. Create an CTA Bus Tracker API Key [here](https://www.ctabustracker.com/home). Add the key as a variable in a `.env` file locally named in the format `BUS_API_KEY="[key]"`
1. Add any raw pattern data already downloaded to `data/patterns_raw/`. 
    * Download our archive [here](https://cta-stop-watch-bucket-do.nyc3.cdn.digitaloceanspaces.com/cta-stop-watch-files/public/patterns_raw.zip).
1. Download any historic processed data and add to `data/processed_by_pid/`. 
    * Download our archive [here](https://cta-stop-watch-bucket-do.nyc3.cdn.digitaloceanspaces.com/cta-stop-watch-files/public/processed_by_pid.zip)
1. Download a `rt_to_pid.parquet` xwalk and add to `data/`
    * Download our version [here](https://cta-stop-watch-bucket-do.nyc3.cdn.digitaloceanspaces.com/cta-stop-watch-files/public/rt_to_pid.parquet)
1. Add any timetable data already downloaded to `data/clean_timetables/`. 
    * Download our archive [here](https://cta-stop-watch-bucket-do.nyc3.cdn.digitaloceanspaces.com/cta-stop-watch-files/public/clean_timetables.zip).

1. Run `uv run python -m main -c` to update config file after files have been added or manually update `config.json` file. `utils.create_config()`. When prompted, enter in date you want to start download. (for testing, put two days ago.)

### Processing Trips
Run `uv run python -m main -p process`

This function runs the following:

1. `process_trips.update_data(config.MAX_DATE, today)`: Downloads all data from max date of downloaded data to present. Keeps an archive in `data/raw_trips`. Also saves staging data to use for the daily script in `data/staging/days/*`, `data/staging/pids/*` and `data/staging/current_days_download`.parquet`
1. `process_trips.update_patterns()`: Attempts to download and process new patterns that are in the data that are not present. Adds patterns to `data/patterns/raw_patterns/*`. Processes all patterns and adds to `data/patterns/current_patterns`
1. `calculate_stop_time.calculate_patterns(pids)`: Interpolates bus stop times for new trips. Adds files for each pattern to store for the month in `data/staging/trips/{pid}/*`. File format is `trips_{pid}_{pull_date}.parquet`
1. Run `utils.create_config()` to update max date and the list of existing patterns.
1. Update rt to pid xwalk with `util.create_rt_pid_xwalk()`
1. Run `clear_staging(folders=["staging/days", "staging/pids", "raw_trips"],files=["staging/current_days_download.parquet"])` to clear staging files for the next run.

Outputs
* a folder of processed trips for that run in `data/staging/trips/{pid}/`. File format is `trips_{pid}_{pull_date}.parquet`

See `process.log` for details of a run

### Metrics Creation

Run `uv run python -m main -p metrics local`.

This function runs the following:

1. `update_metrics.combine_recent_trips()`: Merges monthly staged trips in `data/staging/trips/{pid}/*` with historic trips in `data/processed_by_pid/`
1. `metrics_utils.create_rt_pid_xwalk()`: Updates a xwalk of all route and pattern combos for metric creation.
1. `update_schedule.update_schedule()`: Downloads the current schedule to `data/timetables/feed_{date}.zip`. Creates timetables from the schedule in `data/timetables/current_timetables`. Then appends the new schedule to the historic schedules in `data/clean_timetables/*`.
1. `utils.clear_staging(["timetables/current_timetables"])`: Remove current schedules to prepare for subsequent run. 
1. `update_metrics.update_metrics('all')`: Grabs the processed trips from `data/processed_by_pid/` and re calculates metrics. Metrics tables is sent to  `data/metrics/*`
1. [For internal use only] If run with second arg as `remote` instead of `local`, will store processed_by_pid to `s3://../processed_by_pid`, clean_timetables to `s3://../clean_timetables`, patterns/patterns_raw to `s3://../patterns_raw`, and staging/timetables/feed_{today}.zip to `s3://../historic_feeds` (need to create this folder) using `store_data.store_all_data()`

Outputs
* update historic timetables in `data/clean_timetables/` with current timetable. Overwrites existing files.
* combines new processed trips with historic processed trips in `data/processed_by_pid`. Overwrites existing files.
* Downloads a snapshot of the current schedule in `data/timetables/feed_{date}.zip`
* 

See `metrics.log` for details of run

