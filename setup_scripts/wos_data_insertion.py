import sys
import os

# Adjust sys.path before any other imports
current_path = os.path.abspath(os.path.dirname(__file__))
project_root = os.path.abspath(os.path.join(current_path, '..'))
sys.path.insert(0, project_root)

import logging
from logging_config import setup_logging
from config import DATABASE_PATH, FILE_PATH
from wos_preprocessing import read_and_preprocess
import sqlite3
import pandas as pd

setup_logging()  # Initialize logging


def insert_data_to_db(df, db_path='data/project_data.db'):
    try:
        conn = sqlite3.connect(db_path)
        logging.info("Connected to database successfully.")

        c = conn.cursor()

        # Convert text fields to lowercase
        df['doi'] = df['doi'].str.lower()
        df['title'] = df['title'].str.lower()
        df['full_name'] = df['full_name'].apply(lambda x: [name.lower() for name in x])
        df['keywords'] = df['keywords'].apply(lambda x: [keyword.lower() for keyword in x if keyword])
        df['publisher'] = df['publisher'].str.lower()
        df['institutions'] = df['institutions'].apply(lambda x: [institution.lower() for institution in x if institution])

        # Insert data into the papers table
        df[['doi', 'title', 'publication_year', 'times_cited']].to_sql('papers', conn, if_exists='append', index=False)
        logging.info("Data inserted into papers table successfully.")

        # Retrieve paper DOIs for relationship mapping
        paper_dois = pd.read_sql_query("SELECT doi FROM papers", conn)
        logging.info("Paper DOIs retrieved successfully.")

        # Prepare and insert authors
        authors = set([item for sublist in df['full_name'] for item in sublist])
        author_df = pd.DataFrame({'full_name': list(authors)})
        author_df['full_name'] = author_df['full_name'].str.lower()
        author_df.to_sql('authors', conn, if_exists='append', index=False)
        logging.info("Authors data inserted successfully.")

        # Retrieve author IDs for relationship mapping
        author_ids = pd.read_sql_query("SELECT id, full_name FROM authors", conn)
        logging.info("Author IDs retrieved successfully.")

        # Insert keywords
        keywords = set([item for sublist in df['keywords'] for item in sublist])
        keyword_df = pd.DataFrame({'keyword': list(keywords)})
        keyword_df['keyword'] = keyword_df['keyword'].str.lower()
        keyword_df.to_sql('keywords', conn, if_exists='append', index=False)
        logging.info("Keywords inserted successfully.")

        # Retrieve keyword IDs for relationship mapping
        keyword_ids = pd.read_sql_query("SELECT id, keyword FROM keywords", conn)
        logging.info("Keyword IDs retrieved successfully.")

        # Insert publishers
        publishers = set(df['publisher'].dropna())
        publisher_df = pd.DataFrame({'name': list(publishers)})
        publisher_df['name'] = publisher_df['name'].str.lower()
        publisher_df.to_sql('publishers', conn, if_exists='append', index=False)
        logging.info("Publishers data inserted successfully.")

        # Retrieve publisher IDs for relationship mapping
        publisher_ids = pd.read_sql_query("SELECT id, name FROM publishers", conn)
        logging.info("Publisher IDs retrieved successfully.")

        # Insert institutions
        institutions = set([item for sublist in df['institutions'] for item in sublist if item])
        institution_df = pd.DataFrame({'name': list(institutions)})
        institution_df['name'] = institution_df['name'].str.lower()
        institution_df.to_sql('institutions', conn, if_exists='append', index=False)
        logging.info("Institutions data inserted successfully.")

        # Retrieve institution IDs for relationship mapping
        institution_ids = pd.read_sql_query("SELECT id, name FROM institutions", conn)
        logging.info("Institution IDs retrieved successfully.")

        # Insert into authorship table
        authorship = []
        for index, row in df.iterrows():
            paper_doi = row['doi']
            paper_id = paper_dois[paper_dois['doi'] == paper_doi].index[0] + 1  # Using DOI as paper ID
            for author in row['full_name']:
                author_id = author_ids[author_ids['full_name'] == author.lower()].iloc[0]['id']
                authorship.append((author_id, paper_id))
        authorship_df = pd.DataFrame(authorship, columns=['author_id', 'paper_id'])
        authorship_df.to_sql('authorship', conn, if_exists='append', index=False)
        logging.info("Authorship data inserted successfully.")

        # Insert into paper_keywords table
        paper_keywords = []
        for index, row in df.iterrows():
            paper_doi = row['doi']
            paper_id = paper_dois[paper_dois['doi'] == paper_doi].index[0] + 1  # Using DOI as paper ID
            for keyword in row['keywords']:
                keyword_id = keyword_ids[keyword_ids['keyword'] == keyword.lower()].iloc[0]['id']
                paper_keywords.append((paper_id, keyword_id))
        paper_keywords_df = pd.DataFrame(paper_keywords, columns=['paper_id', 'keyword_id'])
        paper_keywords_df.to_sql('paper_keywords', conn, if_exists='append', index=False)
        logging.info("Paper keywords data inserted successfully.")

        conn.commit()
        logging.info("All database transactions committed successfully.")

    except sqlite3.DatabaseError as e:
        logging.error(f"Database error occurred: {e}")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
    finally:
        if conn:
            conn.close()
            logging.info("Database connection closed.")

if __name__ == "__main__":
    try:
        logging.info(f"Processing file: {FILE_PATH}")
        processed_data = read_and_preprocess(FILE_PATH)
        insert_data_to_db(processed_data)
        logging.info(f"Completed processing and inserting data for file: {FILE_PATH}")
        logging.info("All files processed and inserted successfully.")
    except Exception as e:
        logging.error(f"Failed to process and insert data: {e}")
