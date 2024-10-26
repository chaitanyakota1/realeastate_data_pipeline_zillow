import propertyfields
import zipwiseproperties
import time
import pandas as pd
import csv
from datetime import datetime
import os
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

# Setup logging
def setup_logging():
    today_date = datetime.now().strftime("%Y-%m-%d")
    log_directory = f"logger/{today_date}"
    if not os.path.exists(log_directory):
        os.makedirs(log_directory)  # Create the directory if it does not exist

    log_filename = os.path.join(log_directory, 'scraping.log')
    # Configure logging to file and console
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s',
                        handlers=[logging.FileHandler(log_filename),
                                  logging.StreamHandler()])

setup_logging()
logging.info("Logging setup complete - logs are being saved.")


# Save extracted property links to a CSV
def save_links_to_csv(links, filename):
    with open(filename, 'a', newline='') as file:
        writer = csv.writer(file)
        for link in links:
            writer.writerow([link])


# Save scraped property details to CSV
def save_property_details_to_csv(property_details, filename):
    with open(filename, 'a', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=[
            'address', 'listed_price', 'MLS #', 'days_on_zillow', 'views', 'saves', 'url', 'timestamp'
        ])
        if file.tell() == 0:  # Write header only if file is empty
            writer.writeheader()
        writer.writerow(property_details)


# Save error details when scraping fails
def save_error_urls(url, error, filename):
    with open(filename, 'a', newline='') as file:
        writer = csv.writer(file)
        if file.tell() == 0:  # Write header only if file is empty
            writer.writerow(['Error URL', 'Error Message'])
        writer.writerow([url, str(error)])


# Helper function to scrape all pages from a given base URL
def scrape_all_pages(zip_code, base_url, total_pages_count, min_beds=None, max_beds=None):
    all_links = []
    for page_number in range(2, total_pages_count + 1):
        url = zipwiseproperties.update_url_with_page(base_url, page_number)
        html, error = zipwiseproperties.fetch_html_with_zyte(url)
        if html:
            links = zipwiseproperties.parse_properties(html)
            if links:
                all_links.extend(links)
                logging.info(f"Added {len(links)} links from page {page_number} for {zip_code}, beds: {min_beds}-{max_beds}")
        else:
            logging.error(f"Failed to fetch HTML for page {page_number} of {zip_code}")
    return all_links


# Modified scrape_properties function
def scrape_properties(zip_code, base_url):
    """
    Scrape property links for a given zip code from the base URL.

    Parameters:
    zip_code (str): The zip code to scrape properties for.
    base_url (str): The base URL to start scraping from.

    Returns:
    list: A list of unique property links for the zipcode.
    """
    all_links = []
    try:
        html, error = zipwiseproperties.fetch_html_with_zyte(base_url)
        if not html:
            logging.error(f"Failed to fetch HTML for {zip_code}")
            return all_links

        # Parse initial page
        links = zipwiseproperties.parse_properties(html)
        if links:
            all_links.extend(links)
            logging.info(f"Added {len(links)} links from base URL")

        total_pages_count = zipwiseproperties.total_pages(html)
        print(f"{zip_code} has {total_pages_count} total pages")
        logging.info(f"{zip_code} has {total_pages_count} total pages")

        # If there are multiple pages, scrape them
        if total_pages_count >= 20:
            price_filters = [
                {"min": 0, "max": 300000},
                {"min": 300001, "max": 400000},
                {"min": 400001, "max": 450000},
                {"min": 450001, "max": 500000},
                {"min": 500001, "max": 600000},
                {"min": 600001, "max": 800000},
                {"min": 800001, "max": None}  # 800000+ (no max filter needed)
            ]
            bed_filters = [
                (0, 0),  # Studio
                (1, 1),  # 1 Bed
                (2, 2),  # 2 Beds
                (3, 3),  # 3 Beds
                (4, 4),  # 4 Beds
                (5, None)  # 5+ Beds (no max filter needed)
            ]

            for price_filter in price_filters:
                filtered_url = zipwiseproperties.update_url_with_price(base_url, price_filter['min'], price_filter['max'])
                logging.info(f"Adding links from price filter {price_filter['min']}-{price_filter['max']}")
                for min_beds, max_beds in bed_filters:
                    bed_filtered_url = zipwiseproperties.update_url_with_beds(filtered_url, min_beds, max_beds)
                    html, error = zipwiseproperties.fetch_html_with_zyte(bed_filtered_url)
                    pages_count = zipwiseproperties.total_pages(html)
                    links = zipwiseproperties.parse_properties(html)
                    if links:
                        all_links.extend(links)
                    logging.info(f"Added {len(links)} links from beds filter {min_beds}-{max_beds}")
                    all_links.extend(scrape_all_pages(zip_code, bed_filtered_url, pages_count, min_beds, max_beds))
        else:
            all_links.extend(scrape_all_pages(zip_code, base_url, total_pages_count))

    except Exception as e:
        logging.error(f"Error processing {zip_code}: {e}")
    
    # Remove duplicates before returning
    unique_links = list(set(all_links))
    logging.info(f"Total unique links scraped for {zip_code}: {len(unique_links)}")
    
    return unique_links


# Process each zip code for property link extraction
def process_zip_code(zip_code, filename):
    base_url = zipwiseproperties.generate_zipcode_url(zip_code)
    if not base_url:
        logging.error(f"Failed to generate base URL for {zip_code}")
        return 
    links = scrape_properties(zip_code, base_url)
    if links:
        save_links_to_csv(links, filename)
        logging.info(f"Saved {len(links)} links for zip code: {zip_code}")
    else:
        logging.warning(f"No links found for zip code: {zip_code}")


# Process each MSA (Metropolitan Statistical Area)
def process_msa(msa_name, msa_zipcodes, filename):
    # Ensure directory exists
    os.makedirs(os.path.dirname(filename), exist_ok=True)

    with open(filename, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['Property Link'])

    # Process zip codes in parallel
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(process_zip_code, zip_code, filename) for zip_code in msa_zipcodes]
        for future in as_completed(futures):
            try: 
                future.result()  # Wait for each future to complete
            except Exception as e:
                logging.error(f"Error processing zip code {futures[future]}: {e}")

    # Remove duplicates
    df_links = pd.read_csv(filename)
    df_links.drop_duplicates(inplace=True)
    df_links.to_csv(filename, index=False)
    logging.info(f"Removed duplicates. Total unique links saved: {len(df_links)}")


# Process property URL and scrape details
def process_url(url, results_file, error_file, success_counter, error_counter):
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    html, error = zipwiseproperties.fetch_html_with_zyte(url)  # Fetch HTML or receive error info

    if html:
        property_details = propertyfields.parsePropertyFields(html, url)
        property_details['timestamp'] = timestamp
        save_property_details_to_csv(property_details, results_file)
        success_counter.append(1)  # Increment success counter
    else:
        logging.error(f"Failed to process URL: {url}. Error: {error}")
        save_error_urls(url, error, error_file)
        error_counter.append(1)  # Increment error counter


# Scrape properties for a city based on extracted URLs
def scrape_city(city, csv_filename, max_workers):
    today = datetime.now().strftime("%Y-%m-%d")
    results_directory = f'scraped_results/{city}/{today}'
    os.makedirs(results_directory, exist_ok=True)
    results_file = f'{results_directory}/{city}_property_details.csv'

    error_directory = f'scraped_errors/{city}/{today}'
    os.makedirs(error_directory, exist_ok=True)
    error_file = f'{error_directory}/error_property_urls.csv'

    start_time = datetime.now()
    print(f"Scraping started at: {start_time} for {city}")

    property_links = []
    with open(csv_filename, 'r') as file:
        reader = csv.reader(file)
        next(reader)  # Skip the header
        property_links.extend([row[0] for row in reader])

    logging.info(f"Total URLs to process: {len(property_links)}")

    success_counter = []  # List to track successful URLs
    error_counter = []  # List to track error URLs

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(process_url, url, results_file, error_file, success_counter, error_counter) for url in property_links]
        
        for future in futures:
            future.result()

    end_time = datetime.now()
    duration = end_time - start_time

    total_processed = len(success_counter) + len(error_counter)
    logging.info(f"Total URLs processed: {total_processed}")
    logging.info(f"Successfully processed URLs: {len(success_counter)}")
    logging.info(f"Error URLs: {len(error_counter)}")

    print(f"All property details saved to {results_file}")
    print(f"URLs with errors saved to {error_file}")
    print(f"Time taken for scraping {city}: {duration.total_seconds()} seconds")
    
# Retry Failed URLS
def retry_failed_urls(city, max_workers):
    today = datetime.now().strftime("%Y-%m-%d")
    error_directory = f'scraped_errors/{city}/{today}'
    error_file = f'{error_directory}/error_property_urls.csv'
    
    if not os.path.exists(error_file):
        logging.error(f"No error file found for {city}. Skipping retry.")
        return

    # Load error URLs
    with open(error_file, 'r') as file:
        reader = csv.reader(file)
        next(reader)  # Skip the header
        error_urls = [row[0] for row in reader]

    if not error_urls:
        logging.info(f"No URLs to retry for {city}.")
        return

    logging.info(f"Retrying {len(error_urls)} failed URLs for {city}.")
    
    results_directory = f'scraped_results/{city}/{today}'
    os.makedirs(results_directory, exist_ok=True)
    results_file = f'{results_directory}/{city}_property_details.csv'  # Same results file

    # Reinitialize the error file for this retry run
    os.remove(error_file)  # Remove the old error file to avoid confusion in new retries
    os.makedirs(error_directory, exist_ok=True)

    success_counter = []  # List to track successful URLs
    error_counter = []  # List to track error URLs

    # Process error URLs in parallel
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(process_url, url, results_file, error_file, success_counter, error_counter) for url in error_urls]
        
        for future in futures:
            future.result()

    logging.info(f"Retry process completed. {len(success_counter)} URLs retried successfully, {len(error_counter)} failed again.")
    print(f"Retry completed for {city}. Successfully processed URLs: {len(success_counter)}, Errors: {len(error_counter)}")
    
# Main function to extract URLs and then scrape properties MSA by MSA
def main():
    # Define the mapping from MSA to the desired city folder names
    msa_to_city_mapping = {
        "Boston-Cambridge-Newton, MA-NH": "boston",
    }

    zipcodes_df = pd.read_csv('data/zip_codes_by_msa.csv', dtype={'GEOID_ZCTA5_20': str})
    zipcodes_df['GEOID_ZCTA5_20'] = zipcodes_df['GEOID_ZCTA5_20'].str.zfill(5)

    today = datetime.now().strftime("%Y-%m-%d")
    
    # Loop through each MSA using the mapping dictionary
    for msa, city in msa_to_city_mapping.items():
        filename = f'property_links/{today}/{msa.replace(",", "").replace("-", " ").lower()}_properties.csv'
        msa_zipcodes = zipcodes_df[zipcodes_df['CBSA Title_x'] == msa]['GEOID_ZCTA5_20'].unique()

        logging.info(f"Starting scraping for {msa}")
        start_time = datetime.now()

        # Step 1: Extract property URLs for the MSA
        process_msa(msa, msa_zipcodes, filename)

        # Step 2: Scrape properties for the extracted URLs for the MSA
        scrape_city(city, filename, max_workers=14)

        # Step 3: Retry failed URLs
        retry_failed_urls(city, max_workers=14)

        end_time = datetime.now()
        logging.info(f"Time taken for {msa}: {end_time - start_time}")
        
# Execute the main function
if __name__ == "__main__":
    main()
