import pandas as pd
import sqlite3
import torch
from torch_geometric.data import HeteroData
from network_preprocess import preprocess_data, preprocess_features

# Adjust sys.path before any other imports
import os
import sys
current_path = os.path.abspath(os.path.dirname(__file__))
project_root = os.path.abspath(os.path.join(current_path, '..'))
sys.path.insert(0, project_root)
from config import DATABASE_PATH

# Database connection setup
conn = sqlite3.connect(DATABASE_PATH)

def load_data(table_name, connection):
    """Load all columns for a table dynamically."""
    query = f"SELECT * FROM {table_name}"
    return pd.read_sql_query(query, connection)

def get_primary_key(column_info):
    """Identify and return the primary key based on column info."""
    for column in column_info:
        if column[5]:  # The sixth element is the 'pk' indicator (1 if true)
            return column[1]  # The second element is the column name
    return None

def get_foreign_keys(connection):
    """Retrieve all foreign key relationships from the database dynamically."""
    cursor = connection.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    
    edge_specs = []
    for (table,) in tables:
        cursor.execute(f"PRAGMA table_info({table})")
        column_info = cursor.fetchall()
        primary_key = get_primary_key(column_info)

        cursor.execute(f"PRAGMA foreign_key_list({table})")
        fks = cursor.fetchall()
        for fk in fks:
            src_table = table
            dst_table = fk[2]
            src_col = primary_key if primary_key else fk[3]
            dst_col = fk[4]
            if src_table in tables and dst_table in tables:  # Ensure both tables are present
                edge_specs.append((src_table, dst_table, src_col, dst_col))
    return edge_specs

# Load all tables dynamically
tables = [table[0] for table in conn.execute("SELECT name FROM sqlite_master WHERE type='table';").fetchall()]
nodes = {table: load_data(table, conn) for table in tables}

# Preprocess node data
nodes = preprocess_data(nodes)

# Dynamic edge specifications based on foreign keys
edge_specs = get_foreign_keys(conn)

# Construct HeteroData dynamically
data = HeteroData()
for node_type, df in nodes.items():
    primary_key = get_primary_key(conn.execute(f"PRAGMA table_info({node_type})").fetchall())
    if primary_key is None:
        primary_key = df.columns[0]  # Default to the first column if primary key is not present

    # Preprocess DataFrame features
    processed_df = preprocess_features(df.drop(primary_key, axis=1))

    # Handle primary key conversion
    if df[primary_key].dtype == 'object':
        df[primary_key], _ = pd.factorize(df[primary_key])

    # Convert to tensor
    data[node_type].x = torch.tensor(processed_df.values, dtype=torch.float)
    data[node_type].node_id = torch.tensor(df[primary_key].values, dtype=torch.int)

# Setup edges in HeteroData
for (src_table, dst_table, src_col, dst_col) in edge_specs:
    query = f"SELECT {src_col}, {dst_col} FROM {src_table}"
    df = pd.read_sql_query(query, conn)
    src_indices = pd.Index(nodes[src_table][src_col])
    dst_indices = pd.Index(nodes[dst_table][dst_col])
    source_indices = src_indices.get_indexer(df[src_col])
    target_indices = dst_indices.get_indexer(df[dst_col])

    edge_index = torch.tensor([source_indices, target_indices], dtype=torch.long)
    data[src_table, dst_table].edge_index = edge_index

# Verify data structure
print(data)