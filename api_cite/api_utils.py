import sys
import os
import logging
import requests
from scholarly import scholarly
from config_private import API_KEY  # Import the API key securely

# Append the directory above 'src' to sys.path to allow imports from the main project directory
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from logging_config import setup_logging  # Import the centralized logging configuration

# Setup logging using the centralized logging configuration
setup_logging()

logger = logging.getLogger(__name__)

def fetch_citations_by_doi(doi):
    """Fetches citations for a given DOI using the OpenCitations API."""
    logger = logging.getLogger(__name__)
    logger.debug(f"Fetching citations for DOI: {doi}")
    url = f"https://opencitations.net/index/api/v2/citations/doi:{doi}"
    headers = {
        'authorization': API_KEY,
        'Accept': 'application/json'
    }
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Will raise an exception for HTTP error codes
        return response.json()
    except requests.exceptions.HTTPError as e:
        logger.error(f"HTTP error fetching citations for DOI {doi}: {str(e)}")
    except requests.exceptions.RequestException as e:
        logger.error(f"Request exception fetching citations for DOI {doi}: {str(e)}")
    return []


def parse_citation_data(citation_data):
    """Parses the JSON data returned from the OpenCitations API to extract relevant citation details."""
    parsed_data = []
    for citation in citation_data:
        citing_details = citation.get('citing', '').split(' ')
        cited_details = citation.get('cited', '').split(' ')
        
        # Extract DOIs from the citation details, assuming the DOI always follows the prefix 'doi:'
        citing_doi = next((detail.split(':')[-1] for detail in citing_details if 'doi:' in detail), None)
        cited_doi = next((detail.split(':')[-1] for detail in cited_details if 'doi:' in detail), None)

        # Construct a dictionary with the extracted data
        parsed_entry = {
            'citing_doi': citing_doi,
            'cited_doi': cited_doi,
        }
        parsed_data.append(parsed_entry)

    return parsed_data

def fetch_h_index(author_name):
    """Fetch h-index information for an author using scholarly."""
    try:
        search_query = scholarly.search_author(author_name)
        author = next(search_query, None)
        if author:
            author = scholarly.fill(author)
            return {
                "name": author_name,
                "hindex": author.get('hindex', None),
                "hindex5y": author.get('hindex5y', None),
                "i10index": author.get('i10index', None),
                "i10index5y": author.get('i10index5y', None)
            }
        else:
            logger.warning(f"No author found for name: {author_name}")
            return None
    except Exception as e:
        logger.error(f"Error fetching h-index for {author_name}: {str(e)}")
        return None