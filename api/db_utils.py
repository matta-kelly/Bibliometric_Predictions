import sqlite3
import logging
import pandas as pd
import json
from logging_config import setup_logging

# Setup logging
setup_logging()
logger = logging.getLogger(__name__)

# Cache dictionary to store journal names and their IDs during the process
journal_cache = {}
# Cache dictionary to store keyword names and their IDs during the process
keyword_cache = {}

def insert_paper(data, connection):
    """Insert paper data into the database."""
    try:
        cursor = connection.cursor()
        cursor.execute("""
            INSERT OR IGNORE INTO Papers (
                doi, paper_id, title, year, citation_count, reference_count, influential_citation_count, journal_id, embedding
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            data.get('doi'), data.get('paper_id'), data.get('title'),
            data.get('year'), data.get('citation_count'), data.get('reference_count'),
            data.get('influential_citation_count'), data.get('journal_id'),
            json.dumps(data.get('embedding'))  # Serialize embedding to JSON
        ))
        connection.commit()
        #logger.info(f"Paper inserted/updated: {data['doi']}")
    except sqlite3.IntegrityError as e:
        logger.error(f"Integrity error during paper insert: {e}")
    except Exception as e:
        logger.error(f"Failed to insert paper data: {e}")

def insert_author(data, connection):
    try:
        cursor = connection.cursor()
        cursor.execute("""
            INSERT OR IGNORE INTO Authors (
                author_id, name, paperCount, citationCount, hIndex
            ) VALUES (?, ?, ?, ?, ?)
        """, (
            data.get('author_id'), data.get('name'),
            data.get('paperCount'), data.get('citationCount'), data.get('hIndex')
        ))
        connection.commit()
        #logger.info(f"Author inserted/updated: {data['name']} with ID: {data['author_id']}")
    except sqlite3.IntegrityError as e:
        logger.error(f"Integrity error during author insert: {e}")
    except Exception as e:
        logger.error(f"Failed to insert author data: {e}")

def insert_authorship(author_id, doi, connection):
    logger.debug(f"Inserting authorship: author_id={author_id}, doi={doi}")
    try:
        cursor = connection.cursor()
        cursor.execute("""
            INSERT OR IGNORE INTO Authorship (
                author_id, doi
            ) VALUES (?, ?)
        """, (author_id, doi))
        connection.commit()
        #logger.info(f"Authorship inserted/updated for author_id: {author_id}, doi: {doi}")
    except sqlite3.IntegrityError as e:
        logger.error(f"Integrity error during authorship insert: {e}")
    except Exception as e:
        logger.error(f"Failed to insert authorship data: {e}")

def insert_journal(journal_name, connection):
    global journal_cache

    # Check if the journal is already in the cache
    if journal_name in journal_cache:
        return journal_cache[journal_name]

    try:
        cursor = connection.cursor()
        # Check if the journal is already in the database
        cursor.execute("SELECT journal_id FROM Journals WHERE name = ?", (journal_name,))
        journal_id = cursor.fetchone()

        if journal_id:
            journal_id = journal_id[0]
        else:
            # Insert the journal into the database
            cursor.execute("INSERT OR IGNORE INTO Journals (name) VALUES (?)", (journal_name,))
            connection.commit()
            journal_id = cursor.lastrowid

        # Update the cache with the journal
        journal_cache[journal_name] = journal_id
        logger.info(f"Journal inserted/updated: {journal_name} with ID: {journal_id}")
        return journal_id
    except sqlite3.IntegrityError as e:
        logger.error(f"Integrity error during journal insert: {e}")
        return None
    except Exception as e:
        logger.error(f"Failed to insert journal data: {e}")
        return None

def insert_keywords(keywords, connection):
    global keyword_cache

    cursor = connection.cursor()
    keyword_ids = []

    for keyword in keywords:
        # Check if the keyword is already in the cache
        if keyword in keyword_cache:
            keyword_ids.append(keyword_cache[keyword])
            continue

        # Check if the keyword is already in the database
        cursor.execute("SELECT id FROM Keywords WHERE keyword = ?", (keyword,))
        keyword_id = cursor.fetchone()

        if keyword_id:
            keyword_id = keyword_id[0]
        else:
            # Insert the keyword into the database
            cursor.execute("INSERT OR IGNORE INTO Keywords (keyword) VALUES (?)", (keyword,))
            connection.commit()
            keyword_id = cursor.lastrowid

        # Update the cache with the keyword
        keyword_cache[keyword] = keyword_id
        keyword_ids.append(keyword_id)

        #logger.info(f"Keyword inserted/updated: {keyword} with ID: {keyword_id}")

    return keyword_ids

def link_paper_keywords(paper_doi, keyword_ids, connection):
    """Associates keywords with a paper in the PaperKeywords table."""
    cursor = connection.cursor()
    for keyword_id in keyword_ids:
        cursor.execute("""
            INSERT OR IGNORE INTO PaperKeywords (paper_id, keyword_id)
            VALUES (?, ?)
        """, (paper_doi, keyword_id))
    connection.commit()

def get_all_dois(connection):
    """Retrieve all DOIs from the database."""
    cursor = connection.cursor()
    cursor.execute("SELECT doi FROM Papers")
    dois = [row[0] for row in cursor.fetchall()]
    return dois

def is_doi_in_dataset(doi, connection):
    """Check if a DOI exists in the database."""
    cursor = connection.cursor()
    cursor.execute("SELECT EXISTS(SELECT 1 FROM Papers WHERE doi = ?)", (doi,))
    exists = cursor.fetchone()[0]
    return exists == 1

def insert_citation(citing_doi, cited_doi, connection):
    """Insert a citation relationship into the database if it does not already exist."""
    try:
        cursor = connection.cursor()
        cursor.execute("""
            INSERT OR IGNORE INTO Citations (citing_doi, cited_doi) VALUES (?, ?)
        """, (citing_doi, cited_doi))
        connection.commit()
    except sqlite3.IntegrityError as e:
        connection.rollback()
        logger.error(f"Failed to insert citation from {citing_doi} to {cited_doi}: {e}")
    except Exception as e:
        logger.error(f"An unexpected error occurred: {str(e)}")