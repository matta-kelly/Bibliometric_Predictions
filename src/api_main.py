import sqlite3

def is_doi_in_dataset(doi, connection):
    cursor = connection.cursor()
    cursor.execute("SELECT EXISTS(SELECT 1 FROM papers WHERE doi = ?)", (doi,))
    return cursor.fetchone()[0]

def insert_citation(citing_doi, cited_doi, connection):
    try:
        cursor = connection.cursor()
        cursor.execute("INSERT INTO citations (citing_doi, cited_doi) VALUES (?, ?)", (citing_doi, cited_doi))
        connection.commit()
    except sqlite3.IntegrityError as e:
        log_error(f"Failed to insert citation from {citing_doi} to {cited_doi}: {e}")