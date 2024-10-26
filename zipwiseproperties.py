import requests
import json
import urllib.parse
import time
from bs4 import BeautifulSoup
from base64 import b64decode
import logging

# Import the API key from config.py
from config import ZYTE_API_KEY

def fetch_html_with_zyte(url, max_retries=5, initial_retry_delay=1):
    """
    Fetches HTML content of a URL using Zyte API with retry logic.

    Args:
        url (str): The URL to fetch HTML content from.
        max_retries (int): The maximum number of retries in case of failures.
        initial_retry_delay (int): The initial delay before retrying.

    Returns:
        tuple: HTML content if successful, error message if failed.
    """
    retry_count = 0
    retry_delay = initial_retry_delay
    last_error_message = None

    while retry_count < max_retries:
        try:
            response = requests.post(
                "https://api.zyte.com/v1/extract",
                auth=(ZYTE_API_KEY, ""),
                json={"url": url, "httpResponseBody": True},
                timeout=30
            )

            if response.status_code in [429, 503, 520]:
                logging.warning(f"HTTP {response.status_code} received for URL {url}. Retrying... ({retry_count + 1}/{max_retries})")
                time.sleep(retry_delay)
                retry_delay *= 2
                retry_count += 1
                last_error_message = f"{response.status_code}: {response.reason}"
                continue
    
            response.raise_for_status()
            http_response_body = b64decode(response.json()["httpResponseBody"])
            return http_response_body.decode('utf-8'), None
        except requests.exceptions.Timeout as e:
            last_error_message = f"Timeout: {str(e)}"
            logging.error(f"Timeout error for URL {url}: {last_error_message}")
            retry_count += 1
        except requests.exceptions.RequestException as e:
            error_message = f"{e.response.status_code if e.response else 'No response'}: {str(e)}"
            last_error_message = error_message
            logging.error(f"Error for URL {url}: {error_message}")
            retry_count += 1
        except Exception as e:
            last_error_message = str(e)
            logging.error(f"Unexpected error for URL {url}: {last_error_message}")
            break
    logging.info(f"Max retries reached for URL {url}.")
    return None, last_error_message

def fetch_zipcode_url(zipcode):
    """
    Fetches HTML content for a given zipcode from Zillow.

    Args:
        zipcode (str): The zipcode to fetch the HTML content for.

    Returns:
        str: HTML content of the Zillow page for the given zipcode.
    """
    base_url = f"https://www.zillow.com/homes/{zipcode}_rb/"
    html, error = fetch_html_with_zyte(base_url)
    return html


def parse_properties(html):
    """
    Parses property links from the HTML content.

    Args:
        html (str): The HTML content to parse.

    Returns:
        list: A list of property detail URLs.
    """
    soup = BeautifulSoup(html, 'html.parser')
    data_script = soup.find('script', id='__NEXT_DATA__')
    if data_script:
        data_json = json.loads(data_script.string)
        property_data = data_json['props']['pageProps']['searchPageState']['cat1']['searchResults']['listResults']
        links = [property['detailUrl'] for property in property_data]
        return links
    else:
        return None

def total_pages(html):
    """
    Extracts the total number of pages from the HTML content of Zillow search.

    Args:
        html (str): The HTML content to parse.

    Returns:
        int: The total number of pages.
    """
    soup = BeautifulSoup(html, 'html.parser')
    data_script = soup.find('script', id='__NEXT_DATA__')
    data_json = json.loads(data_script.string)
    total_pages = data_json['props']['pageProps']['searchPageState']['cat1']['searchList']['totalPages']
    return total_pages

def update_url_with_price(base_url, min_price, max_price=None, min_mp=0, max_mp=None):
    """
    Updates the base URL with the specified price and monthly payment (mp) filters.

    Args:
        base_url (str): The base URL to update.
        min_price (int): The minimum price.
        max_price (int or None): The maximum price (optional).
        min_mp (int): The minimum monthly payment.
        max_mp (int or None): The maximum monthly payment (optional).

    Returns:
        str: The updated URL with the specified price and mp filters.
    """
    # Parse the base URL and search query state
    url_parts = base_url.split('?searchQueryState=')
    base_part = url_parts[0].rstrip('/')
    search_query_state = json.loads(urllib.parse.unquote(url_parts[1]))

    # Update the price filter in the search query state
    if max_price is None:
        search_query_state['filterState']['price'] = {"min": min_price}
    else:
        search_query_state['filterState']['price'] = {"min": min_price, "max": max_price}

    # Update the mp filter in the search query state
    if max_mp is None:
        search_query_state['filterState']['mp'] = {"min": min_mp}
    else:
        search_query_state['filterState']['mp'] = {"min": min_mp, "max": max_mp}

    # Reconstruct the full URL with the updated search query state
    encoded_query_state = urllib.parse.quote(json.dumps(search_query_state))
    updated_url = f"{base_part}/?searchQueryState={encoded_query_state}"
    return updated_url

def update_url_with_beds(base_url, min_beds, max_beds=None):
    """
    Updates the base URL with the specified bed filter.

    Args:
        base_url (str): The base URL to update.
        min_beds (int): The minimum number of beds.
        max_beds (int or None): The maximum number of beds (optional).

    Returns:
        str: The updated URL with the specified bed filter.
    """
    # Parse the base URL and search query state
    url_parts = base_url.split('?searchQueryState=')
    base_part = url_parts[0].rstrip('/')
    search_query_state = json.loads(urllib.parse.unquote(url_parts[1]))

    # Update the beds filter in the search query state
    if max_beds is None:
        # If max_beds is not provided, only set min_beds
        search_query_state['filterState']['beds'] = {"min": min_beds}
    else:
        # Set both min_beds and max_beds
        search_query_state['filterState']['beds'] = {"min": min_beds, "max": max_beds}

    # Reconstruct the full URL with the updated search query state
    encoded_query_state = urllib.parse.quote(json.dumps(search_query_state))
    updated_url = f"{base_part}/?searchQueryState={encoded_query_state}"
    return updated_url

def update_url_with_page(base_url, page_number):
    """
    Updates the base URL with the specified page number.

    Args:
        base_url (str): The base URL to update.
        page_number (int): The page number to set in the URL.

    Returns:
        str: The updated URL with the specified page number.
    """
    url_parts = base_url.split('?searchQueryState=')
    base_part = url_parts[0].rstrip('/')
    search_query_state = json.loads(urllib.parse.unquote(url_parts[1]))

    search_query_state['pagination'] = {"currentPage": page_number}

    encoded_query_state = urllib.parse.quote(json.dumps(search_query_state))
    updated_url = f"{base_part}/{page_number}_p/?searchQueryState={encoded_query_state}"
    return updated_url

def parse_queryState(html):
    """
    Parses the query state from the HTML content and modifies it as needed.

    Args:
        html (str): The HTML content to parse.

    Returns:
        tuple: A tuple containing the base URL and modified query state.
    """
    soup = BeautifulSoup(html, 'html.parser')
    data_script = soup.find('script', id='__NEXT_DATA__')
    if data_script:
        data_json = json.loads(data_script.string)
        baseurl = data_json['props']['pageProps']['searchPageState']['searchPageSeoObject']['baseUrl']
        queryState = data_json['props']['pageProps']['searchPageState']['queryState']

        queryState['ah'] = {"value": True}
        queryState['isListVisible'] = True
        queryState['mapZoom'] = 15
        queryState['pagination'] = {}

        queryState['filterState']['mf'] = {"value": False}
        queryState['filterState']['con'] = {"value": False}
        queryState['filterState']['land'] = {"value": False}
        queryState['filterState']['apa'] = {"value": False}
        queryState['filterState']['manu'] = {"value": False}
        queryState['filterState']['apco'] = {"value": False}
        
        if 'isAllHomes' in queryState['filterState']:
            del queryState['filterState']['isAllHomes']

        if 'sortSelection' in queryState['filterState']:
            queryState['filterState']['sort'] = queryState['filterState'].pop('sortSelection')
        
        return baseurl, queryState
    else:
        logging.error("Error parsing query state: '__NEXT_DATA__' script not found.")
        return None, None

def parse_soldQueryState(html):
    """
    Parses the query state from the HTML content and modifies it as needed for scraping sold properties.

    Args:
        html (str): The HTML content to parse.

    Returns:
        tuple: A tuple containing the base URL and modified query state.
    """
    
    soup = BeautifulSoup(html, 'html.parser')
    data_script = soup.find('script', id='__NEXT_DATA__')
    if data_script:
        data_json = json.loads(data_script.string)
        baseurl = data_json['props']['pageProps']['searchPageState']['searchPageSeoObject']['baseUrl']
        queryState = data_json['props']['pageProps']['searchPageState']['queryState']

        # Adding Default Filters as per zillow 
        # queryState['ah'] = {"value": True}
        queryState['isListVisible'] = True
        queryState['mapZoom'] = 15
        queryState['pagination'] = {}
    
        # Filtering only Homes and Townhomes
        queryState['filterState']['mf'] = {"value": False}
        queryState['filterState']['con'] = {"value": False}
        queryState['filterState']['land'] = {"value": False}
        queryState['filterState']['apa'] = {"value": False}
        queryState['filterState']['manu'] = {"value": False}
        queryState['filterState']['rs'] = {"value": True}
        queryState['filterState']['fsba'] = {"value": False}
        queryState['filterState']['fsbo'] = {"value": False}
        queryState['filterState']['nc'] = {"value": False}
        queryState['filterState']['cmsn'] = {"value": False}
        queryState['filterState']['auc'] = {"value": False}
        queryState['filterState']['fore'] = {"value": False}
        queryState['filterState']['doz'] = {"value": '24m'}
        
        # Modify the queryState as required
        if 'isAllHomes' in queryState['filterState']:
            del queryState['filterState']['isAllHomes']

        if 'sortSelection' in queryState['filterState']:
            queryState['filterState']['sort'] = queryState['filterState'].pop('sortSelection')
        
        # links = [property['detailUrl'] for property in property_data]
        return baseurl,queryState
    else:
        logging.error("Error parsing query state: '__NEXT_DATA__' script not found.")
        return None, None

def generate_zipcode_url_sold(zipcode):
    """
    Fetch the search url for sold properties of a given zipcode.

    Parameters:
    zipcode (str): The zipcode for which to fetch the HTML content.

    Returns:
    str: Final redirected URL on zillow
    """
    html = fetch_zipcode_url(zipcode)
    if not html:
        logging.error(f"No HTML content fetched for zipcode: {zipcode}")
        return None
    baseurl,queryState = parse_soldQueryState(html)
    if not baseurl or not queryState:
        logging.error(f"Parsing failed for HTML content of zipcode: {zipcode}")
        return None
    queryState_json = json.dumps(queryState)
    final_url = f"https://www.zillow.com{baseurl}sold/?searchQueryState={queryState_json}"
    return final_url

def generate_zipcode_url(zipcode):
    """
    Fetch the search URL for a given zipcode.

    Parameters:
    zipcode (str): The zipcode for which to fetch the HTML content.

    Returns:
    str: Final redirected URL on Zillow.
    """
    html = fetch_zipcode_url(zipcode)
    if not html:
        logging.error(f"No HTML content fetched for zipcode: {zipcode}")
        return None
    baseurl, queryState = parse_queryState(html)
    if not baseurl or not queryState:
        # Data not available at source (Cant parse)
        logging.error(f"Parsing failed for HTML content of zipcode: {zipcode}")
        return None
    queryState_json = json.dumps(queryState)
    final_url = f"https://www.zillow.com/homes{baseurl}?searchQueryState={queryState_json}"
    return final_url
