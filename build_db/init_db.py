import sqlite3
import sys
import os

# Adjust sys.path before any other imports
current_path = os.path.abspath(os.path.dirname(__file__))  # Path of the current script
project_root = os.path.abspath(os.path.join(current_path, '..'))  # Parent directory of the current script
sys.path.insert(0, project_root)  # Add project root to the start of the search path

from config import DATABASE_PATH

def create_database():
    conn = sqlite3.connect(DATABASE_PATH)
    c = conn.cursor()

    # Drop tables if they exist to avoid conflicts
    c.execute('DROP TABLE IF EXISTS Journals')
    c.execute('DROP TABLE IF EXISTS Authors')
    c.execute('DROP TABLE IF EXISTS Papers')
    c.execute('DROP TABLE IF EXISTS Authorship')
    c.execute('DROP TABLE IF EXISTS Citations')
    c.execute('DROP TABLE IF EXISTS Keywords')
    c.execute('DROP TABLE IF EXISTS PaperKeywords')
    
    # Create Journals Table
    c.execute('''
        CREATE TABLE IF NOT EXISTS Journals (
            journal_id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE
        )
    ''')

    # Create Authors Table
    c.execute('''
        CREATE TABLE IF NOT EXISTS Authors (
            author_id TEXT PRIMARY KEY,
            name TEXT,
            paperCount INT,
            citationCount INT,
            hIndex INT
        )
    ''')

    # Create Papers Table
    c.execute('''
        CREATE TABLE IF NOT EXISTS Papers (
            doi TEXT PRIMARY KEY,
            paper_id TEXT,
            title TEXT,
            title_embedding TEXT,
            year INTEGER,
            citation_count INTEGER,
            reference_count INTEGER,
            influential_citation_count INTEGER,
            journal_id INTEGER,
            embedding TEXT,
            FOREIGN KEY (journal_id) REFERENCES Journals(journal_id)
        )
    ''')

    # Create Authorship Table
    c.execute('''
        CREATE TABLE IF NOT EXISTS Authorship (
            author_id TEXT,
            doi TEXT,
            FOREIGN KEY (author_id) REFERENCES Authors(author_id),
            FOREIGN KEY (doi) REFERENCES Papers(doi)
        )
    ''')


    # Create Citations Table
    c.execute('''
        CREATE TABLE IF NOT EXISTS Citations (
            citing_doi TEXT,
            cited_doi TEXT,
            FOREIGN KEY (citing_doi) REFERENCES Papers(doi),
            FOREIGN KEY (cited_doi) REFERENCES Papers(doi)
        )
    ''')

    # Create Keywords Table
    c.execute('''
        CREATE TABLE IF NOT EXISTS Keywords (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            keyword TEXT UNIQUE,
            embedding TEXT 
        )
    ''')

    # Create PaperKeywords Table
    c.execute('''
        CREATE TABLE IF NOT EXISTS PaperKeywords (
            paper_id TEXT,
            keyword_id INTEGER,
            FOREIGN KEY (paper_id) REFERENCES Papers(doi),
            FOREIGN KEY (keyword_id) REFERENCES Keywords(id)
        )
    ''')

    conn.commit()
    return conn

# Call the function to create the database and tables
if __name__ == '__main__':
    conn = create_database()
    print("Database and tables created successfully.")
    conn.close()
