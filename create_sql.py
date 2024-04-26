import pandas as pd
import sqlite3
import re

def preprocess_authors(authors_str):
    """Splits author names and returns a list."""
    return [author.strip() for author in authors_str.split(';')] if pd.notnull(authors_str) else []

def preprocess_title(title):
    """Cleans title and prepares for NLP."""
    title = title.lower()
    title = re.sub(r'[^\w\s]', '', title)  # Remove punctuation
    title = re.sub(r'\s+', ' ', title)  # Replace multiple whitespaces with single space
    return title.strip()

def preprocess_keywords(keywords_str):
    """Splits keywords into a list and cleans them."""
    if pd.notnull(keywords_str):
        keywords = [keyword.strip().lower() for keyword in keywords_str.split(';')]
        keywords = [re.sub(r'[^\w\s]', '', keyword) for keyword in keywords]  # Remove punctuation
        return keywords
    else:
        return []

def preprocess_times_cited(times_cited_str):
    """Converts times cited to integer."""
    return int(times_cited_str) if pd.notnull(times_cited_str) else 0

def preprocess_publisher(publisher_str):
    """Cleans publisher string."""
    return publisher_str.strip() if pd.notnull(publisher_str) else None

def preprocess_city(city_str):
    """Cleans city string."""
    return city_str.strip() if pd.notnull(city_str) else None

def preprocess_doi(doi_str):
    """Cleans DOI string."""
    return doi_str.strip() if pd.notnull(doi_str) else None

def preprocess_publication_year(year):
    """Converts publication year to integer."""
    return int(year)

def preprocess_institutions(institutions_str):
    """Splits institution names and returns a list."""
    return [institution.strip() for institution in institutions_str.split(';')] if pd.notnull(institutions_str) else []

def read_and_preprocess(file_path):
    # Read the necessary columns from the CSV file
    df = pd.read_csv(file_path, usecols=[
        'Author Full Names', 'Article Title', 'Author Keywords', 
        'Publisher', 'Times Cited, All Databases', 'Publisher City', 
        'Publication Year', 'DOI', 'Affiliations'
    ])
    
    # Apply preprocessing functions and immediately rename to match SQL schema
    processed_df = pd.DataFrame({
        'title': df['Article Title'].apply(preprocess_title),
        'publication_year': df['Publication Year'].apply(preprocess_publication_year),
        'times_cited': df['Times Cited, All Databases'].apply(preprocess_times_cited),
        'doi': df['DOI'].apply(preprocess_doi),
        'publisher': df['Publisher'].apply(preprocess_publisher),
        'city': df['Publisher City'].apply(preprocess_city),
        'authors': df['Author Full Names'].apply(preprocess_authors),
        'keywords': df['Author Keywords'].apply(preprocess_keywords),
        'institutions': df['Affiliations'].apply(preprocess_institutions),
    })

    return processed_df


def create_database():
    conn = sqlite3.connect('project_data.db')
    c = conn.cursor()
    
    # Create Papers Table
    c.execute('''
        CREATE TABLE IF NOT EXISTS papers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT,
            publication_year INTEGER,
            times_cited INTEGER,
            doi TEXT,
            title_novelty REAL,
            keyword_novelty REAL,
            altmetric_score REAL,
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

    # Create Keywords Table
    c.execute('''
        CREATE TABLE IF NOT EXISTS keywords (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            keyword TEXT
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

    conn.commit()
    return conn

def insert_data_to_db(df, conn):
    # Prepare the data for insertion into the database
    # Here, authors and keywords will be stored as a semicolon-separated string
    df['Authors'] = df['Authors'].apply(lambda x: ';'.join(x))
    df['Keywords'] = df['Keywords'].apply(lambda x: ';'.join(x))
    
    # Insert data into the papers table
    df[['Title', 'Publication Year', 'Times Cited', 'DOI']].to_sql('papers', conn, if_exists='append', index=False)
    
    # Insert data into the authors table
    authors = set(df['Authors'].explode())
    author_df = pd.DataFrame({'full_name': list(authors)})
    author_df.to_sql('authors', conn, if_exists='append', index=False)
    
    # Insert data into the journals table
    journals = set(df['Journal Title'])
    journal_df = pd.DataFrame({'title': list(journals)})
    journal_df.to_sql('journals', conn, if_exists='append', index=False)
    
    # Insert data into the keywords table
    keywords = set(df['Keywords'].explode())
    keyword_df = pd.DataFrame({'keyword': list(keywords)})
    keyword_df.to_sql('keywords', conn, if_exists='append', index=False)
    
    # Insert data into the publishers table
    publishers = set(df['Publisher'])
    publisher_df = pd.DataFrame({'name': list(publishers)})
    publisher_df.to_sql('publishers', conn, if_exists='append', index=False)
    
    # Insert data into the authorship table
    authorship = []
    for idx, row in df.iterrows():
        paper_id = idx + 1  # Assuming paper ids start from 1
        for author in row['Authors']:
            author_id = author_df[author_df['full_name'] == author].index[0] + 1  # Assuming author ids start from 1
            authorship.append((author_id, paper_id))
    authorship_df = pd.DataFrame(authorship, columns=['author_id', 'paper_id'])
    authorship_df.to_sql('authorship', conn, if_exists='append', index=False)
    
    # Insert data into the paper_keywords table
    paper_keywords = []
    for idx, row in df.iterrows():
        paper_id = idx + 1  # Assuming paper ids start from 1
        for keyword in row['Keywords']:
            keyword_id = keyword_df[keyword_df['keyword'] == keyword].index[0] + 1  # Assuming keyword ids start from 1
            paper_keywords.append((paper_id, keyword_id))
    paper_keywords_df = pd.DataFrame(paper_keywords, columns=['paper_id', 'keyword_id'])
    paper_keywords_df.to_sql('paper_keywords', conn, if_exists='append', index=False)


def main():
    conn = create_database()
    file_paths = ['C:/Users/matta/Code/.vscode/BibliometricNetwork/data/se-500.csv', 'C:/Users/matta/Code/.vscode/BibliometricNetwork/data/de-500.csv', 'C:/Users/matta/Code/.vscode/BibliometricNetwork/data/ce-500.csv']
    for file_path in file_paths: 
        df = read_and_preprocess(file_path)
        insert_data_to_db(df, conn)
    conn.close()

if __name__ == "__main__":
    main()
