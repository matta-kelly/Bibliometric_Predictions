import sys
import os
import logging
from logging_config import setup_logging
import sqlite3
import pandas as pd
import re

# Adjust path to ensure module visibility
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))

setup_logging()  # Initialize logging

def preprocess_authors(authors_str):
    return [author.strip() for author in authors_str.split(';')] if pd.notnull(authors_str) else []

def preprocess_title(title):
    title = title.lower()
    title = re.sub(r'[^\w\s]', '', title)  # Remove punctuation
    title = re.sub(r'\s+', ' ', title)  # Replace multiple whitespaces with single space
    return title.strip()

def preprocess_keywords(keywords_str):
    if pd.notnull(keywords_str):
        keywords = [keyword.strip().lower() for keyword in keywords_str.split(';')]
        keywords = [re.sub(r'[^\w\s]', '', keyword) for keyword in keywords]  # Remove punctuation
        return keywords
    else:
        return []

def preprocess_times_cited(times_cited_str):
    return int(times_cited_str) if pd.notnull(times_cited_str) else 0

def preprocess_publisher(publisher_str):
    return publisher_str.strip() if pd.notnull(publisher_str) else None

def preprocess_city(city_str):
    return city_str.strip() if pd.notnull(city_str) else None

def preprocess_doi(doi_str):
    return doi_str.strip() if pd.notnull(doi_str) else None

def preprocess_publication_year(year):
    return int(year)

def preprocess_institutions(institutions_str):
    return [institution.strip() for institution in institutions_str.split(';')] if pd.notnull(institutions_str) else []

def read_and_preprocess(file_path):
    try:
        logging.info(f"Reading data from {file_path}")
        df = pd.read_csv(file_path, usecols=[
            'Author Full Names', 'Article Title', 'Author Keywords', 
            'Publisher', 'Times Cited, All Databases', 'Publisher City', 
            'Publication Year', 'DOI', 'Affiliations'
        ])
        logging.info("Data read successfully")
        
        # Drop rows where DOI is missing
        df.dropna(subset=['DOI'], inplace=True)
        
        processed_df = pd.DataFrame({
            'full_name': df['Author Full Names'].apply(preprocess_authors),  
            'title': df['Article Title'].apply(preprocess_title),
            'publication_year': df['Publication Year'].apply(preprocess_publication_year),
            'times_cited': df['Times Cited, All Databases'].apply(preprocess_times_cited),
            'doi': df['DOI'].apply(preprocess_doi),
            'publisher': df['Publisher'].apply(preprocess_publisher),
            'city': df['Publisher City'].apply(preprocess_city),
            'keywords': df['Author Keywords'].apply(preprocess_keywords),
            'institutions': df['Affiliations'].apply(preprocess_institutions),
        })
        logging.info(f"Data preprocessing completed for {file_path}")
        return processed_df
    except Exception as e:
        logging.error(f"Failed to process data from {file_path}: {e}")
        raise
