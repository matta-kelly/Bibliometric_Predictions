import sqlite3
import json
import logging
from scholarly import scholarly
from config_private import DATABASE_PATH

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_author_from_db(connection):
    """Fetch one author from the database."""
    cursor = connection.cursor()
    cursor.execute("SELECT id, full_name FROM authors LIMIT 1")
    return cursor.fetchone()

def fetch_h_index(author_name):
    """Fetch h-index information using scholarly."""
    try:
        search_query = scholarly.search_author(author_name)
        author = next(search_query, None)
        if author:
            author = scholarly.fill(author)
            return {
                "name": author_name,
                "hindex": author.get('hindex', None),
                "hindex5y": author.get('hindex5y', None),
                "i10index": author.get('i10index', None),
                "i10index5y": author.get('i10index5y', None)
            }
        else:
            logger.warning(f"No author found for name: {author_name}")
            return None
    except Exception as e:
        logger.error(f"Error fetching h-index for {author_name}: {str(e)}")
        return None

def main():
    # Connect to the database
    connection = sqlite3.connect(DATABASE_PATH)
    logger.info("Database connection established.")
    
    try:
        author = get_author_from_db(connection)
        if author:
            author_id, author_name = author
            logger.info(f"Fetched author: {author_name} (ID: {author_id})")
            h_index_info = fetch_h_index(author_name)
            if h_index_info:
                logger.info(f"H-index information: {json.dumps(h_index_info, indent=2)}")
            else:
                logger.info("No h-index information found.")
        else:
            logger.info("No author found in the database.")
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
    finally:
        connection.close()
        logger.info("Database connection closed.")

if __name__ == "__main__":
    main()
