import sqlite3
import json
import logging
import os
import sys

# Adjust sys.path before any other imports
current_path = os.path.abspath(os.path.dirname(__file__))  # Path of the current script
project_root = os.path.abspath(os.path.join(current_path, '..'))  # Parent directory of the current script
sys.path.insert(0, project_root)  # Add project root to the start of the search path
from logging_config import setup_logging

# Setup logging
setup_logging()
logger = logging.getLogger(__name__)

def get_connection():
    from config import DATABASE_PATH
    return sqlite3.connect(DATABASE_PATH)

def insert_title_embedding(doi, embedding):
    """Insert title embedding into the database for a specific paper."""
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE Papers
            SET title_embedding = ?
            WHERE doi = ?
        """, (json.dumps(embedding), doi))
        conn.commit()
        conn.close()
        logger.info(f"Title embedding updated for DOI: {doi}")
    except Exception as e:
        logger.error(f"Failed to update title embedding for DOI {doi}: {e}")

def insert_keyword_embedding(keyword_id, embedding):
    """Insert keyword embedding into the database."""
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE Keywords
            SET embedding = ?
            WHERE id = ?
        """, (json.dumps(embedding), keyword_id))
        conn.commit()
        conn.close()
        logger.info(f"Keyword embedding updated for ID: {keyword_id}")
    except Exception as e:
        logger.error(f"Failed to update keyword embedding for ID {keyword_id}: {e}")

def get_papers_without_title_embedding():
    """Retrieve papers that do not have title embeddings."""
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT doi, title
            FROM Papers
            WHERE title_embedding IS NULL
        """)
        papers = cursor.fetchall()
        conn.close()
        return papers
    except Exception as e:
        logger.error(f"Failed to retrieve papers without title embedding: {e}")
        return []

def get_keywords_without_embedding():
    """Retrieve keywords that do not have embeddings."""
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT id, keyword
            FROM Keywords
            WHERE embedding IS NULL
        """)
        keywords = cursor.fetchall()
        conn.close()
        return keywords
    except Exception as e:
        logger.error(f"Failed to retrieve keywords without embedding: {e}")
        return []

def update_paper_age():
    """Update the age of papers based on the current year."""
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE Papers
            SET age = strftime('%Y', 'now') - year
        """)
        conn.commit()
        conn.close()
        logger.info("Paper ages updated successfully.")
    except Exception as e:
        logger.error(f"Failed to update paper ages: {e}")

def normalize_field(table, field):
    """Normalize a numerical field in a specified table."""
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(f"SELECT MIN({field}), MAX({field}) FROM {table}")
        min_val, max_val = cursor.fetchone()
        if min_val is not None and max_val is not None and min_val != max_val:
            cursor.execute(f"""
                UPDATE {table}
                SET {field} = ({field} - ?) / (?)
            """, (min_val, max_val - min_val))
            conn.commit()
        conn.close()
        logger.info(f"Field {field} in table {table} normalized successfully.")
    except Exception as e:
        logger.error(f"Failed to normalize field {field} in table {table}: {e}")
