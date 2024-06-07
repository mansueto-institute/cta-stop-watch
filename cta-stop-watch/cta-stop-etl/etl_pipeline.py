import pathlib
import os
from calculate_stop_time import process_all_patterns
from scrape_patterns import convert_to_geometries
from qc_pipeline import qc_pipeline


# pull in raw pattern data, process and write it out
PATTERN_DIR = pathlib.Path(__file__).parent / "out/patterns_raw"

all_patterns = os.listdir(PATTERN_DIR)

for pattern in all_patterns:
    # depending on naming convention, remove the file extension
    pattern = pattern.replace(".parquet", "")
    print(f"Processing Pattern: {pattern}")
    convert_to_geometries(pattern)


# this script pulls all needed data from its local source and processes it
# returns written df for each pattern
print("Processing all trips")
process_all_patterns()

# qc script
print("Running QC suite")
qc_pipeline()
