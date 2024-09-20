#!/usr/bin/env bash

# Print repo folder structure
tree -d > repo_folders.txt

# Print repo content including files (except from parquet files)
tree -I "*.parquet" > repo_files.txt 

# https://dev.to/ayon_ssp/tree-command-3c5k