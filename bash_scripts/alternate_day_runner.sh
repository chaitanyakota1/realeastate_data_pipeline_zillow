#!/bin/bash

# File to store the last run date
LAST_RUN_FILE="/Users/chaitanyakota/realeastate_data_pipeline_zillow/data/last_run_date.txt"

# Get today's date
TODAY=$(date +%Y-%m-%d)

# Check if the last run date file exists
if [ -f "$LAST_RUN_FILE" ]; then
    LAST_RUN=$(cat "$LAST_RUN_FILE")
else
    # If the file doesn't exist, assume it hasn't run before
    LAST_RUN=""
fi

# Calculate the number of days since the last run
if [ -n "$LAST_RUN" ]; then
    LAST_RUN_SECONDS=$(date -jf "%Y-%m-%d" "$LAST_RUN" "+%s")
    TODAY_SECONDS=$(date -jf "%Y-%m-%d" "$TODAY" "+%s")
    DAYS_SINCE_LAST_RUN=$(( (TODAY_SECONDS - LAST_RUN_SECONDS) / 86400 ))
else
    DAYS_SINCE_LAST_RUN=0
fi

# Check if today is the day to run the job
if [ "$DAYS_SINCE_LAST_RUN" -ge 2 ] || [ "$LAST_RUN" == "" ]; then
    echo "Running the job today."
    # Run the main job script
    /Users/chaitanyakota/realeastate_data_pipeline_zillow/bash_scripts/start_scrape.sh

    # Update the last run date
    echo "$TODAY" > "$LAST_RUN_FILE"
    echo "Updated last run date to: $TODAY"
else
    echo "Not running the job today. Last run was $DAYS_SINCE_LAST_RUN days ago."
fi
