import sqlite3
from api_utils import (
    fetch_paper_details,
    parse_paper_details,
    fetch_citations,
    parse_citation_data
)
from db_utils import (
    insert_paper,
    insert_author,
    insert_citation,
    insert_authorship,
    insert_journal,
    insert_keywords,
    link_paper_keywords,
    get_all_dois,
    is_doi_in_dataset
)
from config import DATABASE_PATH, FILE_PATH
import logging
from logging_config import setup_logging
import time
import pandas as pd

# Apply centralized logging setup
setup_logging()
logger = logging.getLogger(__name__)

def load_dois_and_keywords():
    df = pd.read_csv(FILE_PATH)
    return df[['DOI', 'Keywords']].dropna()

def count_rows(table_name, connection):
    """Count the number of rows in a specific table."""
    cursor = connection.cursor()
    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    count = cursor.fetchone()[0]
    logger.info(f"Total rows in {table_name}: {count}")
    return count

def validate_paper_data(paper_data):
    required_fields = ['title', 'year', 'authors', 'citationCount', 'referenceCount', 'journal', 'embedding']
    
    if not all(field in paper_data and paper_data[field] for field in required_fields):
        missing_fields = [field for field in required_fields if field not in paper_data or not paper_data[field]]
        logging.error(f"Missing required fields: {missing_fields}")
        return False
    
    for author in paper_data['authors']:
        author_required_fields = ['authorId', 'name', 'paperCount', 'citationCount', 'hIndex']
        if any(field not in author or not author[field] for field in author_required_fields):
            logging.error("Incomplete author data")
            return False
    
    return True            

def process_papers(connection):
    doi_keywords = load_dois_and_keywords()
    papers_processed = 0

    for _, row in doi_keywords.iterrows():
        doi = row['DOI']
        keywords = row['Keywords'].split(';')  # Assuming keywords are separated by semicolons
        paper_details, error_message = fetch_paper_details(doi)
        if paper_details:
            for paper_data in paper_details:
                if validate_paper_data(paper_data):
                    paper = parse_paper_details(paper_data, doi)
                    process_single_paper(paper[0], connection, keywords)
                    papers_processed += 1
                    logger.info(f"Processed {papers_processed} papers.")
                else:
                    logger.error(f"Invalid data for DOI {doi}, skipping.")
        else:
            logger.error(f"No details fetched for DOI {doi}, skipping. Error: {error_message}")

def process_single_paper(paper, connection, keywords):
    if paper.get('journal') and paper['journal'].get('name'):
        journal_id = insert_journal(paper['journal']['name'], connection)
        paper['journal_id'] = journal_id
    insert_paper(paper, connection)
    keyword_ids = insert_keywords(keywords, connection)  # Ensure keyword IDs are returned
    link_paper_keywords(paper['doi'], keyword_ids, connection)  # Use paper['doi'] instead of paper[doi]
    for author in paper.get('authors', []):
        process_single_author(author, paper['doi'], connection)  # Use paper['doi'] instead of paper[doi]

def process_single_author(author, paper_doi, connection):
    # Insert the author's details into the Authors table
    insert_author(author, connection)
    # Link the author to the paper in the Authorship table
    insert_authorship(author['author_id'], paper_doi, connection)

def process_citations(connection):
    """Process citations, excluding already processed DOIs."""
    all_dois = get_all_dois(connection)
    processed_count = 0
    matched_count = 0
    mismatched_count = 0
    failed_count = 0

    # Debug: Print the type and first few elements of all_dois
    logger.info(f"Type of all_dois: {type(all_dois)}")
    logger.info(f"First 10 DOIs: {all_dois[:10]}")

    logger.info(f"Total DOIs to process for citations: {len(all_dois)}")

    for doi in all_dois:
        print(doi)
        try:
            citation_data = fetch_citations(doi)
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
        except Exception as e:
            logger.error(f"Error processing DOI {doi}: {str(e)}")
            failed_count += 1
            time.sleep(1)  # Sleep to handle rate limiting

        # Periodically log progress
        if processed_count % 100 == 0:
            logger.info(f"Progress: {processed_count} DOIs processed, {matched_count} matched, {mismatched_count} mismatched, {failed_count} failed.")

    logger.info("Database connection closed.")
    logger.info(f"Final Report: {matched_count} matched, {mismatched_count} mismatched, {failed_count} failed out of {processed_count} processed DOIs.")

def main():
    with sqlite3.connect(DATABASE_PATH) as connection:
        logging.info("Database connection established.")
        run_papers = False
        run_citations = True

        if run_papers:
            process_papers(connection)
        if run_citations:
            process_citations(connection)

        # Log row counts for each table at the end of the process
        count_rows("Papers", connection)
        count_rows("Authors", connection)
        count_rows("Citations", connection)
        count_rows("Journals", connection)
        count_rows("Keywords", connection)
        count_rows("PaperKeywords", connection)

if __name__ == "__main__":
    main()
