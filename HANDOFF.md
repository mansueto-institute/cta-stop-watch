# CTA Stop Watch — Session Handoff
_Last updated: 2026-04-12_

---

## Current Branch
`fix/ptr-keyerror-download` (branched from `update_server_config`)
Pushed to origin.

---

## What Was Fixed This Session

### 1. Core pipeline bug (calculate_stop_time.py ~line 300)
Bug: `format(processed_trips_count/bad_trips, ".0%")` — `bad_trips` is a list.  
Introduced in commit `0ec79da` on 2025-09-24. Silently caught → ALL trip processing
halted for 6.5 months.  
Fix: `total_trips = processed_trips_count + len(bad_trips)` and use `len(bad_trips)`.

### 2. KeyError 'ptr' in download.py query_cta_api()
Root cause: Expired 32-char API key. New 25-char key added to `.env`.  
Fix: Guard clause `"ptr" in pattern["bustime-response"]` before access.  
Also: archives old pattern to `patterns_historic/pid_{pid}_raw_{YYYY-MM-DD}.parquet`
when stop sequence changes (going forward).

### 3. Daily pattern refresh (process_trips.py update_patterns())
Change: Now checks ALL active PIDs daily, not just new ones.

### 4. backfill.py created
File: `cta-stop-watch/report_automation/backfill.py`  
Processes the 6.5-month gap from `data/raw_trips/*.parquet` directly.  
NOT RUN YET — waiting for historic pattern versioning to be wired in first.

---

## Open Problem: Pattern Versioning for Backfill

341 of 945 PIDs (36%) have different stops today vs. the backfill period
(2025-09-24 to 2026-04-11). The fix is to use historic GTFS feeds as the
source of truth for which stops were active on each date.

---

## Agreed Architecture: GTFS-Backed Historic Patterns

### File naming
```
data/patterns/patterns_historic/pid_{pid}_stop_{feed_start_date}.parquet
data/patterns/patterns_historic/pid_{pid}_segment_{feed_start_date}.parquet
```
Where `feed_start_date` = the GTFS feed's `earliest_calendar_date`.

Example for PID 4110:
```
pid_4110_stop_2025-09-03.parquet   ← from 2025-09-03_2025-11-30.zip
pid_4110_stop_2025-10-20.parquet   ← from 2025-10-20_2025-12-31.zip
pid_4110_stop_2026-03-20.parquet   ← from 2026-03-20_2026-05-31.zip
```

No end date in the filename. The "end" of any version is implicitly the
`feed_start_date` of the next file. Same logic as `dedupe_schedules()`.

### pattern_opener() update (calculate_stop_time.py ~line 372)
Add a `trip_date` parameter. Logic:
1. List all `patterns_historic/pid_{pid}_stop_*.parquet`
2. Parse date from filename (the suffix after the last `_`, before `.parquet`)
3. Keep only files where `feed_start_date <= trip_date`
4. Take the one with the latest feed_start_date (= most recently active version)
5. Fall back to `patterns_current/pid_{pid}_stop.parquet` if no historic match

```python
import glob
from datetime import date

def pattern_opener(pid, trip_date=None, base_path="data/patterns"):
    if trip_date:
        pattern = f"{base_path}/patterns_historic/pid_{pid}_stop_*.parquet"
        candidates = []
        for f in glob.glob(pattern):
            date_str = f.rsplit("_", 1)[-1].replace(".parquet", "")
            try:
                feed_date = date.fromisoformat(date_str)
                if feed_date <= trip_date:
                    candidates.append((feed_date, f))
            except ValueError:
                pass
        if candidates:
            _, best = max(candidates, key=lambda x: x[0])
            return pd.read_parquet(best)
    return pd.read_parquet(f"{base_path}/patterns_current/pid_{pid}_stop.parquet")
```

### What needs updating in process_historic_gtfs.py
Currently writes ONE merged parquet per feed zip (all PIDs combined) to
`archive/scrapers/out/gtfs/`.

Needs to ALSO write per-PID stop + segment files (same format as
`patterns_current/`) tagged with the feed's `earliest_calendar_date`.

The existing `add_patterns_from_archive.py` does the geometry processing
but only for PIDs *missing* from `patterns_current/`. We need a version
that processes ALL PIDs from each feed and writes:
```
patterns_historic/pid_{pid}_stop_{feed_start_date}.parquet
patterns_historic/pid_{pid}_segment_{feed_start_date}.parquet
```

The geometry processing logic lives in:
`archive/cta-stop-etl/add_patterns_from_archive.py` → `convert_to_geometries()`
(same as `process_patterns.py` in the main pipeline)

---

## Implementation Steps (in order)

### Step 1 — Download missing GTFS feeds
Run from `cta-stop-watch/report_automation/` (has the `.env` with TransitLand key):

```python
import os
from dotenv import load_dotenv
from pathlib import Path
from urllib.request import urlretrieve

load_dotenv(Path('.env'))
KEY = os.getenv('TRANSIT_LAND_API_KEY')
BASE = 'https://transit.land/api/v2/rest/feed_versions/'
OUT = Path('../archive/scrapers/inp/historic_gtfs')
OUT.mkdir(exist_ok=True)

feeds = [
    ('c07e68317b3e4452accfc9f09805727a88937458', '2025-10-20', '2025-12-31'),
    ('14d83314a4f56f375d6336a4bb7528df4487b055', '2025-11-19', '2026-01-31'),
    ('8e03d71e0152ea0c012488f6637b362d783a0412', '2025-12-16', '2026-02-28'),
    ('1e2576de6b9537a6dfda10e58477ae401ad1dc74', '2026-01-06', '2026-03-31'),
    ('3fdb2739c597979bd0622b776496cb467d6bd3f4', '2026-03-20', '2026-05-31'),
]
# 2025-09-03_2025-11-30.zip already exists in that directory

for sha1, start, end in feeds:
    out = OUT / f'{start}_{end}.zip'
    if not out.exists():
        print(f'Downloading {out.name}...', end=' ', flush=True)
        urlretrieve(f'{BASE}{sha1}/download?api_key={KEY}', out)
        print(f'done ({out.stat().st_size/1e6:.1f} MB)')
```

### Step 2 — Write a new script: build_historic_patterns.py
New script (suggest placing at `report_automation/build_historic_patterns.py`) that:
1. Iterates over all zips in `archive/scrapers/inp/historic_gtfs/`
2. Extracts `feed_start_date` from filename (`YYYY-MM-DD_YYYY-MM-DD.zip` → first date)
3. For each zip: calls `extract_files_from_zip()` (already in process_historic_gtfs.py)
   then `build_merged_pattern_data()` to get a combined patterns df
4. For each unique PID in that df: calls `convert_to_geometries()` (from add_patterns_from_archive.py)
5. Writes output to `data/patterns/patterns_historic/pid_{pid}_stop_{feed_start_date}.parquet`
   and `pid_{pid}_segment_{feed_start_date}.parquet`

Skip if file already exists (idempotent).

This is assembling existing functions — no new logic needed, just a new orchestration script.

### Step 3 — Update pattern_opener() in calculate_stop_time.py
Replace current implementation (~line 372) with the date-aware version shown above.

Also update callers of `pattern_opener()` to pass `trip_date` where available.
The trip date is in `unique_trip_vehicle_day` or derivable from `bus_stop_time`.

### Step 4 — Run the backfill
```bash
cd cta-stop-watch/report_automation
python backfill.py --start 2025-09-24
# outputs to data/staging/trips/
```

### Step 5 — Sync and compute metrics (on server)
```bash
rsync -av data/staging/trips/ user@server:/path/data/staging/trips/
bash scripts/metrics.sh
```

---

## Other Outstanding Items

### CloudFront data gap
Local `processed_by_pid/` is current through 2026-04-11.
`store_all_data()` in `store_data.py` has never been in cron — last run Nov 2024.
Fix: Run `store_all_data()` manually, then add to end of `scripts/metrics.sh`.

### ghostbus-cta-scrape .env
May still have old 32-char API key:
```bash
sudo cp cta-stop-watch/report_automation/.env ghostbus-cta-scrape/.env
```

---

## Key File Locations
| What | Where |
|------|-------|
| Main pipeline dir | `cta-stop-watch/report_automation/` |
| Raw GPS data | `data/raw_trips/` — 582 daily parquets, 1.9 GB, current through 2026-04-11 |
| Processed trips | `data/processed_by_pid/` — 891 files, 31 GB, stale since Sep 2025 |
| Current patterns | `data/patterns/patterns_current/` |
| Historic patterns | `data/patterns/patterns_historic/` — empty until Step 2 done |
| GTFS feed zips | `archive/scrapers/inp/historic_gtfs/` |
| GTFS processing code | `archive/scrapers/process_historic_gtfs.py` |
| Geometry processing code | `archive/cta-stop-etl/add_patterns_from_archive.py` → convert_to_geometries() |
| Cron scripts | `scripts/daily_combine.sh`, `process.sh`, `metrics.sh` |
| Backfill script | `report_automation/backfill.py` |
