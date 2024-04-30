import sqlite3
from config import DATABASE_PATH

def create_database():
    conn = sqlite3.connect(DATABASE_PATH)
    c = conn.cursor()
    
    # Create Keywords Table
    c.execute('''
        CREATE TABLE IF NOT EXISTS keywords (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            keyword TEXT
        )
    ''')

    # Create Papers Table
    c.execute('''
        CREATE TABLE IF NOT EXISTS papers (
            doi TEXT PRIMARY KEY,
            title TEXT,
            publication_year INTEGER,
            times_cited INTEGER,
            title_novelty REAL,
            keyword_novelty REAL,
            publisher_id INTEGER,
            journal_id INTEGER,
            institution_id INTEGER,
            FOREIGN KEY (publisher_id) REFERENCES publishers(id),
            FOREIGN KEY (journal_id) REFERENCES journals(id),
            FOREIGN KEY (institution_id) REFERENCES institutions(id)
        )
    ''')

    # Create Authors Table
    c.execute('''
        CREATE TABLE IF NOT EXISTS authors (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            full_name TEXT,
            h_index INTEGER
        )
    ''')

    # Create Journals Table
    c.execute('''
        CREATE TABLE IF NOT EXISTS journals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT,
            impact_factor REAL
        )
    ''')

    # Create Institutions Table
    c.execute('''
        CREATE TABLE IF NOT EXISTS institutions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT
        )
    ''')

    # Create Publishers Table
    c.execute('''
        CREATE TABLE IF NOT EXISTS publishers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT
        )
    ''')

    # Create Authorship Table
    c.execute('''
        CREATE TABLE IF NOT EXISTS authorship (
            author_id INTEGER,
            paper_id INTEGER,
            FOREIGN KEY (author_id) REFERENCES authors(id),
            FOREIGN KEY (paper_id) REFERENCES papers(id)
        )
    ''')

    # Create PaperKeywords Table
    c.execute('''
        CREATE TABLE IF NOT EXISTS paper_keywords (
            paper_id INTEGER,
            keyword_id INTEGER,
            FOREIGN KEY (paper_id) REFERENCES papers(id),
            FOREIGN KEY (keyword_id) REFERENCES keywords(id)
        )
    ''')

    # Create Citation
    c.execute('''
        CREATE TABLE IF NOT EXISTS citations (
            citing_paper_doi TEXT,
            cited_paper_doi TEXT,
            FOREIGN KEY (citing_paper_doi) REFERENCES papers(doi),
            FOREIGN KEY (cited_paper_doi) REFERENCES papers(doi)
        )
    ''')


    conn.commit()
    return conn
