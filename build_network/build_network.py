import pandas as pd
import torch
from torch_geometric.data import HeteroData
from torch_geometric.transforms import ToUndirected
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
data['paper'].x = torch.tensor(papers_df.drop(columns=['doi', 'title', 'title_embedding']).values, dtype=torch.float)
data['paper'].node_id = torch.tensor(papers_df['doi'].values, dtype=torch.long)

# Add nodes for Authors
data['author'].x = torch.tensor(authors_df.drop(columns=['author_id']).values, dtype=torch.float)
data['author'].node_id = torch.tensor(authors_df['author_id'].values, dtype=torch.long)

# Add nodes for Keywords
data['keyword'].x = torch.tensor(keywords_df.drop(columns=['id']).values, dtype=torch.float)
data['keyword'].node_id = torch.tensor(keywords_df['id'].values, dtype=torch.long)

# Add nodes for Journals
data['journal'].x = torch.tensor(journals_df.drop(columns=['journal_id']).values, dtype=torch.float)
data['journal'].node_id = torch.tensor(journals_df['journal_id'].values, dtype=torch.long)

# Add edges for Authorship (author <-> paper)
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

# Print the edges before applying ToUndirected to ensure they are correct
print("Edges before ToUndirected:")
print(data)

# Convert undirected edges for all except the 'cites' relationship
undirected_edge_types = [('author', 'writes', 'paper'), ('paper', 'has', 'keyword'), ('journal', 'publishes', 'paper')]
for edge_type in undirected_edge_types:
    data = ToUndirected()(data, edge_types=[edge_type])

# Verify the structure of the heterogeneous data object after transformation
print("Edges after ToUndirected:")
#print(data)

conn.close()
