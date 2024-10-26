from bs4 import BeautifulSoup

def extractPropertyInfo(soup):
    """
    Extracts property address, MLS number, and listed price from the given BeautifulSoup object.

    Args:
        soup (BeautifulSoup): The BeautifulSoup object representing the HTML content of the page.

    Returns:
        tuple: A tuple containing the address, MLS number, and listed price.
    """
    
    try:
        title_text = soup.title.text
        address, mls = title_text.split('|')[:2]
        address = address.strip()
        MLS = mls.strip().split('#')[-1].strip()
    except (AttributeError, ValueError):
        address = "N/A"
        MLS = "N/A"

    meta_tag = soup.find('meta', {'name': 'description'})
    if meta_tag:
        content = meta_tag.get('content', '')
        parts = content.split('$')
        listed_price = parts[1].split()[0] if len(parts) > 1 else "N/A"
    else:
        listed_price = "N/A"

    return address, MLS, listed_price

def extractStats(soup):
    """
    Extracts days on Zillow, views, and saves from the given BeautifulSoup object.

    Args:
        soup (BeautifulSoup): The BeautifulSoup object representing the HTML content of the page.

    Returns:
        tuple: A tuple containing days on Zillow, views, and saves.
    """
    
    stats = soup.select_one('dl[class*=StyledOverviewStats]')
    if stats:
        dt_elements = [dt for dt in stats.find_all('dt') if not dt.get('class')]
        days_on_zillow = dt_elements[0].find('strong').text if len(dt_elements) > 0 else "N/A"
        views = dt_elements[1].find('strong').text if len(dt_elements) > 1 else "N/A"
        saves = dt_elements[2].find('strong').text if len(dt_elements) > 2 else "N/A"
    else:
        days_on_zillow, views, saves = "N/A", "N/A", "N/A"

    return days_on_zillow, views, saves

def parsePropertyFields(html, url):
    """
    Parses property fields from the HTML content of a property page.

    Args:
        html (str): The HTML content of the page.
        url (str): The URL of the page.

    Returns:
        dict: A dictionary containing property details.
    """
    
    soup = BeautifulSoup(html, 'html.parser')

    address, MLS, listed_price = extractPropertyInfo(soup)
    days_on_zillow, views, saves = extractStats(soup)

    return {
        'address': address,
        'listed_price': listed_price,
        'MLS #': MLS,
        'days_on_zillow': days_on_zillow,
        'views': views,
        'saves': saves,
        'url': url
    }
