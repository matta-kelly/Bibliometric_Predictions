import sqlite3
import time
from api_utils import fetch_citations_by_doi, parse_citation_data, fetch_citations_by_doi, parse_citation_data, fetch_h_index
from db_utils import is_doi_in_dataset, insert_citation, is_doi_in_dataset, insert_citation, insert_h_index_to_db, get_all_dois, get_all_authors_from_db
from config import DATABASE_PATH
import logging
from logging_config import setup_logging

# Apply centralized logging setup
setup_logging()

logger = logging.getLogger('api_main')



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

        # Retrieve h-index for authors
        authors = get_all_authors_from_db(connection)
        total_authors = len(authors)
        auth_processed_count = 0
        success_count = 0
        failure_count = 0

        for author_id, author_name in authors:
            try:
                h_index_info = fetch_h_index(author_name)
                if h_index_info:
                    insert_h_index_to_db(h_index_info, connection)
                    success_count += 1
                else:
                    failure_count += 1
                processed_count += 1
                logger.info(f"Processed {auth_processed_count}/{total_authors} authors.")
            except Exception as e:
                logger.error(f"Error processing h-index for author {author_name}: {str(e)}")
                failure_count += 1
                time.sleep(1)  # Sleep to handle rate limiting

    except Exception as e:
        logger.error(f"An overarching error occurred: {str(e)}")
    finally:
        connection.close()
        logger.info("Database connection closed.")
        logger.info(f"Final Report: {matched_count} matched, {mismatched_count} mismatched out of {processed_count} processed.")
        logger.info(f"Author final report: {success_count} matched, {failure_count} mismatched out of {auth_processed_count} processed.")

if __name__ == "__main__":
    main()
