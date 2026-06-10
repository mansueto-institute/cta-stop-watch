#! /bin/bash

cd /home/rmedina_uchicago_edu/cta-stop-watch/cta-stop-watch/ghostbus-cta-scrape
/home/rmedina_uchicago_edu/cta-stop-watch/.venv/bin/python combine_daily_files.py 2>&1 | tee daily_combine.log
