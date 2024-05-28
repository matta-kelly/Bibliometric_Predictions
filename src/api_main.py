import sqlite3
from api_utils import fetch_citations_by_doi, parse_citation_data
from db_utils import is_doi_in_dataset, insert_citation
from config import DATABASE_PATH
import logging
from logging_config import setup_logging  # Import the centralized logging setup

# Apply centralized logging setup
setup_logging()

logger = logging.getLogger('api_main')

def get_all_dois(connection):
    """Retrieve all DOIs from the database to process."""
    cursor = connection.cursor()
    cursor.execute("SELECT doi FROM papers")
    all_dois = [row[0] for row in cursor.fetchall()]
    return all_dois

def main():
    # Connect to the database
    connection = sqlite3.connect(DATABASE_PATH)
    logger.info("Database connection established.")

    try:
        # Fetch all DOIs from the database
        test_doi = ["10.1108/jd-12-2013-0166"]  # Test with a list to simulate multiple DOIs
        logger.info(f"Retrieved {len(test_doi)} DOIs to process.")

        # Process each DOI
        for doi in test_doi:
            citation_data = fetch_citations_by_doi(doi)
            if citation_data:
                parsed_citations = parse_citation_data(citation_data)
                for citation in parsed_citations:
                    citing_doi = citation['citing_doi']
                    cited_doi = citation['cited_doi']
                    if is_doi_in_dataset(citing_doi, connection) and is_doi_in_dataset(cited_doi, connection):
                        insert_citation(citing_doi, cited_doi, connection)
                        logger.info(f"Inserted citation from {citing_doi} to {cited_doi}.")
            else:
                logger.warning(f"No citation data found for DOI {doi}.")
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
    finally:
        # Close the database connection
        connection.close()
        logger.info("Database connection closed.")

if __name__ == "__main__":
    main()
