import sqlite3
import logging

# Set up logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_first_10_keywords(seed_start_year, seed_end_year, connection):
    cursor = connection.cursor()
    query = """
        SELECT p.doi, k.keyword 
        FROM papers p 
        JOIN paper_keywords pk ON p.doi = pk.paper_id 
        JOIN keywords k ON pk.keyword_id = k.id 
        WHERE p.publication_year BETWEEN ? AND ?
        LIMIT 10
    """
    
    logger.debug(f"Executing query: {query}")
    cursor.execute(query, (seed_start_year, seed_end_year))
    results = cursor.fetchall()
    logger.debug(f"Retrieved {len(results)} records from the database.")
    
    for result in results:
        logger.info(f"DOI: {result[0]}, Keyword: {result[1]}")
    
    return results

def main():
    # Configuration
    seed_start_year = 2000
    seed_end_year = 2010

    # Database connection
    logger.info("Database connection established.")
    conn = sqlite3.connect('data/project_data.db')
    
    # Get first 10 keywords
    get_first_10_keywords(seed_start_year, seed_end_year, conn)
    
    logger.info("Database connection closed.")
    conn.close()

if __name__ == "__main__":
    main()
