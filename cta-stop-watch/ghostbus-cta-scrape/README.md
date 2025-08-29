# API Scrapper 

A scrapper for obtaining daily bus data from the CTA API. 

### Recommended Setup
1. [Clone repo](https://docs.github.com/en/repositories/creating-and-managing-repositories/cloning-a-repository)
1. [Install uv](https://docs.astral.sh/uv/getting-started/installation/)
1. `cd` into repo and run `uv sync`
1. `cd` to `cta-stop-watch/cta-stop-watch/ghostbus-cta-scrape`
1. Get a personal key at https://www.ctabustracker.com/home
1. Register your key in the local environment with: 
`$ export CHN_GHOST_BUS_CTA_BUS_TRACKER_API_KEY="YOUR-KEY"`
1. Run with `uv run combine_daily_files.py`

## Automation 
This scrapper runs daily, see [scripts](../../scripts) for more information.