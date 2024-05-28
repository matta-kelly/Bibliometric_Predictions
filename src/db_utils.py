import sqlite3
import logging

# Configure logging for db_utils
logger = logging.getLogger('db_utils')
logger.setLevel(logging.INFO)
handler = logging.FileHandler('../logs/db_utils.log')
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

def is_doi_in_dataset(doi, connection):
    """Check if a DOI exists in the dataset.
    
    Args:
        doi (str): The DOI to check.
        connection (sqlite3.Connection): Active database connection.

    Returns:
        bool: True if the DOI exists, False otherwise.
    """
    cursor = connection.cursor()
    cursor.execute("SELECT EXISTS(SELECT 1 FROM papers WHERE doi = ?)", (doi,))
    exists = cursor.fetchone()[0]
    logger.info(f"Checked existence for DOI {doi}: {'exists' if exists else 'does not exist'}")
    return exists == 1

def insert_citation(citing_doi, cited_doi, connection):
    """Insert a citation relationship into the database.
    
    Args:
        citing_doi (str): The DOI of the citing paper.
        cited_doi (str): The DOI of the cited paper.
        connection (sqlite3.Connection): Active database connection.
    
    Raises:
        sqlite3.IntegrityError: Raised if the insertion fails due to integrity issues.
    """
    try:
        cursor = connection.cursor()
        cursor.execute("INSERT INTO citations (citing_paper_doi, cited_paper_doi) VALUES (?, ?)", (citing_doi, cited_doi))
        connection.commit()
        logger.info(f"Inserted citation from {citing_doi} to {cited_doi}.")
    except sqlite3.IntegrityError as e:
        logger.error(f"Failed to insert citation from {citing_doi} to {cited_doi}: {e}")
        raise

def setup_database_logging():
    """Sets up the logging configuration for the database utilities."""
    logger.setLevel(logging.INFO)
    handler = logging.FileHandler('../logs/db_utils.log')  # Adjust the path as necessary
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

if __name__ == '__main__':
    setup_database_logging()
