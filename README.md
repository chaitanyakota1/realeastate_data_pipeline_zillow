# Zillow Property Scraper

## Overview
This pipeline is a Python based tool to extract real-estate data from Zillow. It uses the Zyte service for HTML fetching and processes the data to save in CSV format. It automates the process of scraping property details for a city of interest. The scraper is configured to run automatically every other day to ensure up-to-date information. Can be used by students or new home owners to track properties price movement.

## Features
- Fetch HTML content using Zyte API
- Parse properties (Links) up for sale and gathers the activity for these properties (price,views,saves)
- Save results and errors to CSV
- Multi-threaded processing for efficiency
- Automated scraping of property data from Zillow scheduled every alternate day
- Configurable job scheduling with `cron` and SLURM.

## Setup

### Prerequisites
- Python 3.x
- [Zyte](https://www.zyte.com/) account for HTML fetching
- Update zyte api key in `config.py`
- Setup a venv 

### Installation
Clone the repository:
    ```bash
    git clone https://github.com/yourusername/property-scraper.git
    ```

## Usage
1. Setting Up the Virtual Environment
    Create and Activate the Virtual Environment
  ```bash
  python3 -m venv webscraping
  source webscraping/bin/activate
  pip install -r requirements.txt
  ```

2. Setup Automation and Run the script: 
    Manually using the following command:
    ```bash
    alternate_day_runner.sh
    ```
    Alternatively, set it up as a cronjob on your machine
    ```bash
    crontab -e
    0 10  * * * /Users/chaitanyakota/zillow_scraping/alternate_day_runner.sh
    # This will run the alternate_day_runner.sh script every day at 10:00 AM and but executes every alternate day
    # alternate_day_runner will submits SLURM job and executes start_scrape.sh which will run scraper.py 
    # The `alternate_day_runner.sh` script will execute `start_scrape.sh` to run `scraper.py`.
    ```


## Automation Setup
1. `alternate_day_runner.sh`: This script checks if the scraper has been run in the last two days and triggers the scraping job if necessary.

3. `start_scrape.sh` : Executes the scraping script within a conda environment.


## File Structure
- `scraper.py`: The main Python file that gets executed every alternate day for getting property links and property fields.
- `propertyfields.py`: Module for parsing property fields.
- `zipwiseproperties.py`: Module for fetching HTML content using Zyte.

### notebooks/
- `sold.ipynb` : Notebook to scrape sale, properties, neighborhood details of all properties sold in zillow in last 24 months.

### bash_scripts/
- `alternate_day_runner.sh` : This script is to run the scraper every alternate day.
- `start_scrape.sh`: Initialising scraping env and runs the scrape


### Folders
- `logger/`: Directory for log files.
- `scraped_results/`: Directory for scraped results.
- `property_links/`: Directory for CSV files with property links.

#### data/
- `zip_codes_by_msa.csv`: Contains zipcodes for a given MSA.
- `last_run_date.txt` : File that stores last scraping date, used to execute scraping every alternate day

## Contributing
Feel free to submit issues or pull requests.
