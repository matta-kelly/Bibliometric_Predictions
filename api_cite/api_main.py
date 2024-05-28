import sqlite3
import time
from api_utils import fetch_citations_by_doi, parse_citation_data
from db_utils import is_doi_in_dataset, insert_citation
from config import DATABASE_PATH
import logging
from logging_config import setup_logging

# Apply centralized logging setup
setup_logging()

logger = logging.getLogger('api_main')

def get_all_dois(connection):
    """Retrieve all DOIs from the database to process."""
    cursor = connection.cursor()
    cursor.execute("SELECT doi FROM papers")
    return [row[0] for row in cursor.fetchall()]

def main():
    # Connect to the database
    connection = sqlite3.connect(DATABASE_PATH)
    logger.info("Database connection established.")

    dois = get_all_dois(connection)  # Use the actual function to fetch all DOIs
    processed_count = 0
    matched_count = 0
    mismatched_count = 0

    try:
        logger.info(f"Retrieved {len(dois)} DOIs to process.")

        for index, doi in enumerate(dois, 1):
            try:
                citation_data = fetch_citations_by_doi(doi)
                if citation_data:
                    parsed_citations = parse_citation_data(citation_data)
                    for citation in parsed_citations:
                        citing_doi = citation['citing_doi']
                        cited_doi = citation['cited_doi']
                        if is_doi_in_dataset(citing_doi, connection) and is_doi_in_dataset(cited_doi, connection):
                            insert_citation(citing_doi, cited_doi, connection)
                            matched_count += 1
                        else:
                            mismatched_count += 1
                else:
                    logger.warning(f"No citation data found for DOI {doi}.")
                processed_count += 1
                logger.info(f"Processed {processed_count}/{len(dois)} DOIs.")
            except Exception as e:
                logger.error(f"Error processing DOI {doi}: {str(e)}")
                time.sleep(1)  # Sleep to handle rate limiting
    except Exception as e:
        logger.error(f"An overarching error occurred: {str(e)}")
    finally:
        connection.close()
        logger.info("Database connection closed.")
        logger.info(f"Final Report: {matched_count} matched, {mismatched_count} mismatched out of {processed_count} processed.")

if __name__ == "__main__":
    main()
