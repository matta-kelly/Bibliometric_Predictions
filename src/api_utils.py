import time  # For rate limiting
import logging  # For logging
import pybliometrics as pb

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def fetch_citation_dois(doi, page=0, limit=10):
    """
    Fetch citation data for a given paper.

    Args:
    - doi: DOI of the paper
    - page: Page number for pagination (default: 0)
    - limit: Maximum number of citations to fetch per page (default: 10)

    Returns:
    - dois: List of DOIs cited by the indexed paper
    """
    try:
        # Check if citation data is already cached
        if doi in citation_cache:
            return citation_cache[doi]

        # Retrieve the document's abstract and citation details in FULL view
        document = pb.AbstractRetrieval(identifier=doi, id_type='doi', view='FULL')
        if hasattr(document, 'references'):
            cited_dois = [ref.doi for ref in document.references if ref.doi]

            # Check if there are more pages to fetch
            if len(cited_dois) == limit:
                next_page = page + 1
                more_dois = fetch_citation_dois(doi, page=next_page, limit=limit)
                cited_dois.extend(more_dois)

            # Cache the fetched citation data
            citation_cache[doi] = cited_dois

            return cited_dois
    except Exception as e:
        logger.error(f"Failed to fetch citation data for DOI {doi}: {e}")
    return []

# Example usage:
if __name__ == "__main__":
    doi = "10.1016/j.softx.2019.100263"  # Example DOI
    cited_dois = fetch_citation_dois(doi)
    print("DOIs cited by the indexed paper:", cited_dois)