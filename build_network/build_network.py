import pandas as pd
import torch
from torch_geometric.data import HeteroData
import sqlite3
import json

from config import DATABASE_PATH

# Function to load data from a specific table
def load_data_from_db(table_name, connection):
    query = f"SELECT * FROM {table_name}"
    return pd.read_sql_query(query, connection)

# Connect to the database
conn = sqlite3.connect(DATABASE_PATH)

# Load data
papers_df = load_data_from_db('Papers', conn)
authors_df = load_data_from_db('Authors', conn)
keywords_df = load_data_from_db('Keywords', conn)
authorship_df = load_data_from_db('Authorship', conn)
citations_df = load_data_from_db('Citations', conn)
paper_keywords_df = load_data_from_db('PaperKeywords', conn)
journals_df = load_data_from_db('Journals', conn)

# Initialize the HeteroData object
data = HeteroData()

# Add nodes for Papers
paper_features = papers_df[['citation_count', 'reference_count', 'influential_citation_count', 'embedding', 'title_embedding']].applymap(json.loads)
data['paper'].x = torch.tensor(paper_features.values.tolist(), dtype=torch.float)
data['paper'].node_id = torch.tensor(papers_df['doi'].values, dtype=torch.long)

# Add nodes for Authors
author_features = authors_df[['paperCount', 'citationCount', 'hIndex']].astype(float)
data['author'].x = torch.tensor(author_features.values.tolist(), dtype=torch.float)
data['author'].node_id = torch.tensor(authors_df['author_id'].values, dtype=torch.long)

# Add nodes for Keywords
keyword_features = keywords_df[['embedding']].applymap(json.loads)
data['keyword'].x = torch.tensor(keyword_features.values.tolist(), dtype=torch.float)
data['keyword'].node_id = torch.tensor(keywords_df['id'].values, dtype=torch.long)

# Add nodes for Journals
journal_features = pd.factorize(journals_df['name'])[0]
data['journal'].x = torch.tensor(journal_features, dtype=torch.long).unsqueeze(1)
data['journal'].node_id = torch.tensor(journals_df['journal_id'].values, dtype=torch.long)

# Add edges for Authorship (paper <-> author)
source_author = authorship_df['author_id'].values
target_paper = authorship_df['doi'].values
data['author', 'writes', 'paper'].edge_index = torch.tensor([source_author, target_paper], dtype=torch.long)

# Add edges for Citations (paper -> paper)
source_cite = citations_df['citing_doi'].values
target_cite = citations_df['cited_doi'].values
data['paper', 'cites', 'paper'].edge_index = torch.tensor([source_cite, target_cite], dtype=torch.long)

# Add edges for PaperKeywords (paper <-> keyword)
source_keyword = paper_keywords_df['paper_id'].values
target_keyword = paper_keywords_df['keyword_id'].values
data['paper', 'has', 'keyword'].edge_index = torch.tensor([source_keyword, target_keyword], dtype=torch.long)

# Add edges for Journals (journal <-> paper)
source_journal = papers_df['journal_id'].values
target_journal = papers_df['doi'].values
data['journal', 'publishes', 'paper'].edge_index = torch.tensor([source_journal, target_journal], dtype=torch.long)

# Verify the structure of the heterogeneous data object
print(data)

conn.close()
