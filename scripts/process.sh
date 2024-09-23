#! /bin/bash

cd /home/divijs_uchicago_edu/cta-stop-watch/cta-stop-watch/ghostbus-cta-scrape
/home/divijs_uchicago_edu/cta-stop-watch/.venv/bin/python combine_daily_files.py

cd /home/divijs_uchicago_edu/cta-stop-watch/cta-stop-watch/report_automation
/home/divijs_uchicago_edu/cta-stop-watch/.venv/bin/python -m main -p process
