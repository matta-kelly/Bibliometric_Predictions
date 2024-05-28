import sqlite3
from api_utils import fetch_doi_data
from db_utils import is_doi_in_dataset, insert_citation
from config import DATABASE_PATH
import logging

# Configure logging
logger = logging.getLogger('api_main')
logger.setLevel(logging.INFO)
handler = logging.FileHandler('../logs/api_main.log')
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

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
        dois = get_all_dois(connection)
        logger.info(f"Retrieved {len(dois)} DOIs to process.")

        # Process each DOI
        for doi in dois:
            result = fetch_doi_data(doi)
            if result and 'search-results' in result and 'entry' in result['search-results']:
                cited_dois = [entry['prism:doi'] for entry in result['search-results']['entry'] if 'prism:doi' in entry]
                for cited_doi in cited_dois:
                    if is_doi_in_dataset(cited_doi, connection):
                        insert_citation(doi, cited_doi, connection)
                        logger.info(f"Inserted citation from {doi} to {cited_doi}.")
            else:
                logger.warning(f"No data found for DOI {doi} or incorrect data format.")
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
    finally:
        # Close the database connection
        connection.close()
        logger.info("Database connection closed.")

if __name__ == "__main__":
    main()
