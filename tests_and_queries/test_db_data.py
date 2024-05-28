import sqlite3

# Connect to the database
conn = sqlite3.connect('data/project_data.db')

# Define a function to execute SQL queries
def execute_query(query):
    cursor = conn.cursor()
    cursor.execute(query)
    result = cursor.fetchall()
    cursor.close()
    return result

# Count the number of rows in each table
tables = ['papers', 'authors', 'keywords', 'publishers', 'institutions', 'authorship', 'paper_keywords', 'citations']
for table in tables:
    count_query = f"SELECT COUNT(*) FROM {table}"
    result = execute_query(count_query)
    print(f"Number of rows in {table}: {result[0][0]}")

# Retrieve sample data from each table
sample_queries = {
    'papers': 'SELECT * FROM papers LIMIT 5',
    'authors': 'SELECT * FROM authors LIMIT 5',
    'keywords': 'SELECT * FROM keywords LIMIT 5',
    'publishers': 'SELECT * FROM publishers LIMIT 5',
    'institutions': 'SELECT * FROM institutions LIMIT 5',
    'authorship': 'SELECT * FROM authorship LIMIT 5',
    'paper_keywords': 'SELECT * FROM paper_keywords LIMIT 5',
    'citations': 'SELECT * FROM citations LIMIT 5',
}
for table, query in sample_queries.items():
    result = execute_query(query)
    print(f"Sample data from {table}: {result}")

# Close the database connection
conn.close()
