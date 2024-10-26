#!/bin/bash

# Create a directory with today's date under batch_jobs
TODAY=$(date +%Y-%m-%d)
OUTPUT_DIR=batch_jobs/$TODAY
mkdir -p $OUTPUT_DIR

# Activate the virtual environment
source webscraping/bin/activate


# Run the Python script with output and error redirection
python scraper.py > $OUTPUT_DIR/test.out 2> $OUTPUT_DIR/test.err
