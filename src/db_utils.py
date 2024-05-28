import sqlite3
import logging
from logging_config import setup_logging  # Import the centralized logging setup

# Apply centralized logging setup
setup_logging()

logger = logging.getLogger('db_utils')

def is_doi_in_dataset(doi, connection):
    """Check if a DOI exists in the dataset."""
    cursor = connection.cursor()
    cursor.execute("SELECT EXISTS(SELECT 1 FROM papers WHERE doi = ?)", (doi,))
    exists = cursor.fetchone()[0]
    #logger.info(f"Checked existence for DOI {doi}: {'exists' if exists else 'does not exist'}")
    return exists == 1

def insert_citation(citing_doi, cited_doi, connection):
    """Insert a citation relationship into the database if it does not already exist."""
    try:
        cursor = connection.cursor()
        # Check if the citation already exists to prevent duplicates
        cursor.execute("SELECT EXISTS(SELECT 1 FROM citations WHERE citing_paper_doi = ? AND cited_paper_doi = ?)", (citing_doi, cited_doi))
        exists = cursor.fetchone()[0]
        if not exists:
            cursor.execute("INSERT INTO citations (citing_paper_doi, cited_paper_doi) VALUES (?, ?)", (citing_doi, cited_doi))
            connection.commit()
            logger.info(f"Inserted citation from {citing_doi} to {cited_doi}.")
        else:
            logger.info(f"Citation from {citing_doi} to {cited_doi} already exists, not inserted.")
    except sqlite3.IntegrityError as e:
        connection.rollback()
        logger.error(f"Failed to insert citation from {citing_doi} to {cited_doi}: {e}")
    except Exception as e:
        logger.error(f"An unexpected error occurred: {str(e)}")

