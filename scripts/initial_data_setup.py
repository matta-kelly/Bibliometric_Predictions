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
from wos_data_insertion import insert_data_to_db
from init_db import create_database
setup_logging()

def main():
    create_database()
    logging.info("Database initialized successfully.")
    logging.info(f"Processing file: {FILE_PATH}")
    processed_data = read_and_preprocess(FILE_PATH)
    insert_data_to_db(processed_data, DATABASE_PATH)
    logging.info(f"Completed processing and insertion for file: {FILE_PATH}")

if __name__ == "__main__":
    main()
