import requests
import time
from config_private import API_KEY, OC_API_KEY  # Import the API key securely
import logging
import os
import sys
import re


# Adjust sys.path before any other imports
current_path = os.path.abspath(os.path.dirname(__file__))  # Path of the current script
project_root = os.path.abspath(os.path.join(current_path, '..'))  # Parent directory of the current script
sys.path.insert(0, project_root)  # Add project root to the start of the search path
from logging_config import setup_logging

# Setup logging
setup_logging()
logger = logging.getLogger(__name__)

def is_valid_doi(doi):
    doi_regex = r'^10.\d{4,9}/[-._;()/:A-Z0-9]+$'
    return re.match(doi_regex, doi, re.IGNORECASE) is not None

def rate_limited_request(url, params=None, headers=None, json_data=None, max_retries=5, initial_wait=1.1):
    headers = headers or {}
    headers['x-api-key'] = API_KEY.strip()
    retries = 0
    wait_time = initial_wait

    while retries < max_retries:
        try:
            response = requests.post(url, params=params, headers=headers, json=json_data)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            logging.error(f"HTTP error: {str(e)} - Status code: {response.status_code}")
            logging.error(f"Response: {response.text}")
            if response.status_code == 400:
                logging.error("Bad request, possibly due to invalid DOI or malformed request.")
                return None
            elif response.status_code == 429:
                logging.error("Too many requests. Retrying after wait.")
                time.sleep(wait_time)
                wait_time *= 2  # Exponential backoff
                retries += 1
            else:
                logging.error(f"Unrecoverable HTTP error: {response.status_code}")
                return None
        except requests.exceptions.RequestException as e:
            logging.error(f"Request exception: {str(e)}")
            time.sleep(wait_time)
            wait_time *= 2  # Exponential backoff
            retries += 1

    logging.error("Max retries exceeded. Request failed.")
    return None

def fetch_paper_details(doi):
    if not doi:
        logging.error("Invalid DOI: DOI cannot be empty.")
        return None, "DOI cannot be empty"

    if not is_valid_doi(doi):
        logging.error(f"Invalid DOI format: {doi}")
        return None, "Invalid DOI format"

    url = 'https://api.semanticscholar.org/graph/v1/paper/batch'
    data = {"ids": [doi]}
    params = {
        'fields': 'title,year,authors,authors.paperCount,authors.citationCount,authors.hIndex,authors.name,citationCount,referenceCount,journal,embedding.specter_v1,influentialCitationCount'
    }
    headers = {'x-api-key': API_KEY.strip()}

    response = rate_limited_request(url, params=params, headers=headers, json_data=data)

    if response is None:
        error_msg = "API response is None"
        logging.error(error_msg)
        return None, error_msg

    if isinstance(response, list):
        paper_data = response
    else:
        error_msg = "Unexpected API response format"
        logging.error(error_msg)
        return None, error_msg

    if not paper_data:
        error_msg = "No data in response"
        logging.error(error_msg)
        return None, error_msg

    return paper_data, None

def parse_paper_details(paper_data, doi):
    if not paper_data:
        return []

    authors = [
        {
            'author_id': author.get('authorId'),
            'name': author.get('name'),
            'paperCount': author.get('paperCount', 0),
            'citationCount': author.get('citationCount', 0),
            'hIndex': author.get('hIndex', 0)
        } 
        for author in paper_data.get('authors', [])
    ]

    paper = {
        'doi': doi,
        'paper_id': paper_data.get('paperId'),
        'title': paper_data.get('title'),
        'year': paper_data.get('year'),
        'citation_count': paper_data.get('citationCount'),
        'reference_count': paper_data.get('referenceCount'),
        'influential_citation_count': paper_data.get('influentialCitationCount'),  # Ensure this is included
        'journal': paper_data.get('journal'),
        'embedding': paper_data.get('embedding'),
        'authors': authors
    }

    return [paper]

# Citations
def fetch_citations(doi):
    """Fetches citations for a given DOI using the OpenCitations API."""
    url = f"https://opencitations.net/index/api/v2/citations/doi:{doi}"
    headers = {
        'authorization': OC_API_KEY,
        'Accept': 'application/json'
    }
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Will raise an exception for HTTP error codes
        #print(f"Response for DOI {doi}: {response.json()}")  # Print the response for debugging
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

        if citing_doi and cited_doi:
            parsed_entry = {
                'citing_doi': citing_doi,
                'cited_doi': cited_doi,
            }
            parsed_data.append(parsed_entry)
        else:
            logger.warning(f"Invalid citation data: {citation}")

    return parsed_data

