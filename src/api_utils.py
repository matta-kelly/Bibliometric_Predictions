import sys
import os
import logging
import requests
import xml.etree.ElementTree as ET
from config_private import API_KEY  # Import the API key securely

# Append the directory above 'src' to sys.path to allow imports from the main project directory
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


from logging_config import setup_logging  # Import the centralized logging configuration

# Setup logging using the centralized logging configuration
setup_logging()

logger = logging.getLogger(__name__)

def fetch_cited_dois(doi):
    """Fetches DOIs of documents that cite the given DOI."""
    logger.debug(f"Fetching cited DOIs for DOI: {doi}")
    try:
        headers = {'X-ELS-APIKey': API_KEY}
        url = f"https://api.elsevier.com/content/abstract/doi/{doi}?view=REF"
        response = requests.get(url, headers=headers)
        #logger.debug(f"Raw Response: {response.text}")  # Log raw response body
        if response.status_code == 200:
            root = ET.fromstring(response.text)
            cited_dois = []
            for reference in root.findall('.//reference'):
                doi_elem = reference.find('.//ce:doi', namespaces={'ce': 'http://prismstandard.org/namespaces/basic/2.0/'})
                if doi_elem is not None:
                    cited_dois.append(doi_elem.text)
            logger.info(f"Successfully fetched cited DOIs for DOI {doi}: {cited_dois}")
            return cited_dois
        else:
            logger.error(f"Failed to fetch cited DOIs for DOI {doi}. Status Code: {response.status_code}")
            return []
    except Exception as e:
        logger.error(f"Error fetching cited DOIs for DOI {doi}: {str(e)}")
        return []


def main():
    test_doi = "10.1016/j.resconrec.2017.09.005"
    logger.debug(f"Testing DOI data fetch for: {test_doi}")
    cited_dois = fetch_cited_dois(test_doi)
    if cited_dois:
        logger.info(f"Cited DOIs: {cited_dois}")
        print("Cited DOIs:", cited_dois)  # Print the list of cited DOIs
    else:
        logger.warning("No cited DOIs retrieved or error occurred.")

if __name__ == "__main__":
    main()
