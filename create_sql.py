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
    df = pd.read_csv(file_path, usecols=[
        'Author Full Names', 'Article Title', 'Author Keywords', 
        'Publisher', 'Times Cited, All Databases', 'Publisher City', 
        'Publication Year', 'DOI', 'Affiliations'
    ])
    
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

    return processed_df

def create_database():
    conn = sqlite3.connect('project_data.db')
    c = conn.cursor()
    
    # Create Papers Table
    c.execute('''
    CREATE TABLE IF NOT EXISTS papers (
        doi TEXT PRIMARY KEY,
        title TEXT,
        publication_year INTEGER,
        times_cited INTEGER,
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
    c = conn.cursor()
    
    # Insert data into the papers table
    df[['doi', 'title', 'publication_year', 'times_cited']].to_sql('papers', conn, if_exists='append', index=False)
    
    # Retrieve paper DOIs for relationship mapping
    paper_dois = pd.read_sql_query("SELECT doi FROM papers", conn)
    
    # Prepare and insert authors
    authors = set([item for sublist in df['full_name'] for item in sublist])
    author_df = pd.DataFrame({'full_name': list(authors)})
    author_df.to_sql('authors', conn, if_exists='append', index=False)
    
    # Retrieve author IDs for relationship mapping
    author_ids = pd.read_sql_query("SELECT id, full_name FROM authors", conn)
    
    # Insert keywords
    keywords = set([item for sublist in df['keywords'] for item in sublist])
    keyword_df = pd.DataFrame({'keyword': list(keywords)})
    keyword_df.to_sql('keywords', conn, if_exists='append', index=False)
    
    # Retrieve keyword IDs for relationship mapping
    keyword_ids = pd.read_sql_query("SELECT id, keyword FROM keywords", conn)
    
    # Insert publishers
    publishers = set(df['publisher'].dropna())
    publisher_df = pd.DataFrame({'name': list(publishers)})
    publisher_df.to_sql('publishers', conn, if_exists='append', index=False)
    
    # Retrieve publisher IDs for relationship mapping
    publisher_ids = pd.read_sql_query("SELECT id, name FROM publishers", conn)
    
    # Insert institutions
    institutions = set([item for sublist in df['institutions'] for item in sublist if item])
    institution_df = pd.DataFrame({'name': list(institutions)})
    institution_df.to_sql('institutions', conn, if_exists='append', index=False)
    
    # Retrieve institution IDs for relationship mapping
    institution_ids = pd.read_sql_query("SELECT id, name FROM institutions", conn)
    
    # Insert into authorship table
    authorship = []
    for index, row in df.iterrows():
        paper_doi = row['doi']
        paper_id = paper_dois[paper_dois['doi'] == paper_doi].index[0] + 1  # Using DOI as paper ID
        for author in row['full_name']:
            author_id = author_ids[author_ids['full_name'] == author].iloc[0]['id']
            authorship.append((author_id, paper_id))
    authorship_df = pd.DataFrame(authorship, columns=['author_id', 'paper_id'])
    authorship_df.to_sql('authorship', conn, if_exists='append', index=False)
        
    # Insert into paper_keywords table
    paper_keywords = []
    for index, row in df.iterrows():
        paper_doi = row['doi']
        paper_id = paper_dois[paper_dois['doi'] == paper_doi].index[0] + 1  # Using DOI as paper ID
        for keyword in row['keywords']:
            keyword_id = keyword_ids[keyword_ids['keyword'] == keyword].iloc[0]['id']
            paper_keywords.append((paper_id, keyword_id))
    paper_keywords_df = pd.DataFrame(paper_keywords, columns=['paper_id', 'keyword_id'])
    paper_keywords_df.to_sql('paper_keywords', conn, if_exists='append', index=False)

    conn.commit()

    c = conn.cursor()
    
    # Insert data into the papers table
    df[['title', 'publication_year', 'times_cited', 'doi']].to_sql('papers', conn, if_exists='append', index=False)
    paper_ids = pd.read_sql_query("SELECT id, doi FROM papers", conn)  # Retrieve paper IDs for relationship mapping
    
    # Prepare and insert authors
    authors = set([item for sublist in df['full_name'] for item in sublist])
    author_df = pd.DataFrame({'full_name': list(authors)})
    author_df.to_sql('authors', conn, if_exists='append', index=False)
    author_ids = pd.read_sql_query("SELECT id, full_name FROM authors", conn)  # Retrieve author IDs for relationship mapping
    
    # Insert keywords
    keywords = set([item for sublist in df['keywords'] for item in sublist])
    keyword_df = pd.DataFrame({'keyword': list(keywords)})
    keyword_df.to_sql('keywords', conn, if_exists='append', index=False)
    keyword_ids = pd.read_sql_query("SELECT id, keyword FROM keywords", conn)  # Retrieve keyword IDs for relationship mapping
    
    # Insert publishers
    publishers = set(df['publisher'].dropna())
    publisher_df = pd.DataFrame({'name': list(publishers)})
    publisher_df.to_sql('publishers', conn, if_exists='append', index=False)
    publisher_ids = pd.read_sql_query("SELECT id, name FROM publishers", conn)  # Retrieve publisher IDs for relationship mapping
    
    # Insert institutions
    institutions = set([item for sublist in df['institutions'] for item in sublist if item])
    institution_df = pd.DataFrame({'name': list(institutions)})
    institution_df.to_sql('institutions', conn, if_exists='append', index=False)
    institution_ids = pd.read_sql_query("SELECT id, name FROM institutions", conn)  # Retrieve institution IDs for relationship mapping
    
    # Insert into authorship table
    authorship = []
    for index, row in df.iterrows():
        paper_doi = row['doi']
        paper_id = paper_ids.loc[paper_ids['doi'] == paper_doi, 'id'].values[0]
        for author in row['full_name']:
            author_id = author_ids.loc[author_ids['full_name'] == author, 'id'].values[0]
            authorship.append((author_id, paper_id))
    authorship_df = pd.DataFrame(authorship, columns=['author_id', 'paper_id'])
    authorship_df.to_sql('authorship', conn, if_exists='append', index=False)
        
    # Insert into paper_keywords table
    paper_keywords = []
    for index, row in df.iterrows():
        paper_doi = row['doi']
        paper_id = paper_ids.loc[paper_ids['doi'] == paper_doi, 'id'].values[0]
        for keyword in row['keywords']:
            keyword_id = keyword_ids.loc[keyword_ids['keyword'] == keyword, 'id'].values[0]
            paper_keywords.append((paper_id, keyword_id))
    paper_keywords_df = pd.DataFrame(paper_keywords, columns=['paper_id', 'keyword_id'])
    paper_keywords_df.to_sql('paper_keywords', conn, if_exists='append', index=False)

    conn.commit()


def main():
    conn = create_database()
    file_paths = ['C:/Users/matta/Code/.vscode/BibliometricNetwork/data/se-500.csv', 'C:/Users/matta/Code/.vscode/BibliometricNetwork/data/de-500.csv', 'C:/Users/matta/Code/.vscode/BibliometricNetwork/data/ce-500.csv']
    for file_path in file_paths:
        df = read_and_preprocess(file_path)
        insert_data_to_db(df, conn)
    conn.close()

if __name__ == "__main__":
    main()
