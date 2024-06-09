import pandas as pd
import os
import logging
import re
from init_db import create_database
from config import FILE_PATH

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# File paths
ce_file = "data/WOS_data/CSV/ce-500.csv"
de_file = "data/WOS_data/CSV/de-500.csv"
se_file = "data/WOS_data/CSV/se-500.csv"
merged_file = FILE_PATH

def preprocess_keywords(keywords_str):
    if pd.notnull(keywords_str):
        keywords = [keyword.strip().lower() for keyword in keywords_str.split(';')]
        keywords = [re.sub(r'[^\w\s]', '', keyword) for keyword in keywords]  # Remove punctuation
        return ';'.join(keywords)  # Join back into a single string
    else:
        return ''

def preprocess_doi(doi_str):
    return doi_str.strip().lower() if pd.notnull(doi_str) else None

def merge_and_preprocess():
    # Read CSV files
    ce_df = pd.read_csv(ce_file)
    de_df = pd.read_csv(de_file)
    se_df = pd.read_csv(se_file)

    # Concatenate CSV files
    merged_df = pd.concat([ce_df, de_df, se_df], ignore_index=True)

    # Drop NaN values in DOI
    merged_df.dropna(subset=['DOI'], inplace=True)

    # Drop duplicate DOIs, keeping the first occurrence
    merged_df.drop_duplicates(subset=['DOI'], keep='first', inplace=True)

    # Preprocess DOI and keywords
    merged_df['DOI'] = merged_df['DOI'].apply(preprocess_doi)
    merged_df['Author Keywords'] = merged_df['Author Keywords'].apply(preprocess_keywords)

    # Keep only necessary columns
    final_df = merged_df[['DOI', 'Author Keywords']].rename(columns={'DOI': 'DOI', 'Author Keywords': 'Keywords'})

    # Save merged and preprocessed data to CSV
    final_df.to_csv(merged_file, index=False)
    logging.info(f"Processed and saved merged data to {merged_file}")

if __name__ == "__main__":
    create_database()
    logging.info("Database initialized successfully.")
    merge_and_preprocess()
    logging.info("Data merging and preprocessing completed successfully.")
