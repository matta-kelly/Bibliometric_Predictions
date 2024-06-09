import sqlite3
import pandas as pd

# Path to your database
DATABASE_PATH = 'data/project_data.db'

# Connect to the database
conn = sqlite3.connect(DATABASE_PATH)

# Define a function to execute SQL queries
def execute_query(query):
    cursor = conn.cursor()
    cursor.execute(query)
    result = cursor.fetchall()
    cursor.close()
    return result

# Count the number of rows in each table
tables = ['Papers', 'Authors', 'Keywords', 'Journals', 'Authorship', 'PaperKeywords', 'Citations']
for table in tables:
    count_query = f"SELECT COUNT(*) FROM {table}"
    result = execute_query(count_query)
    print(f"Number of rows in {table}: {result[0][0]}")

# Retrieve one full row of data from each table
sample_queries = {
    'Papers': 'SELECT * FROM Papers LIMIT 1',
    'Authors': 'SELECT * FROM Authors LIMIT 1',
    'Keywords': 'SELECT * FROM Keywords LIMIT 1',
    'Journals': 'SELECT * FROM Journals LIMIT 1',
    'Authorship': 'SELECT * FROM Authorship LIMIT 1',
    'PaperKeywords': 'SELECT * FROM PaperKeywords LIMIT 1',
    'Citations': 'SELECT * FROM Citations LIMIT 1',
}

for table, query in sample_queries.items():
    result = execute_query(query)
    print(f"Sample data from {table}: {result}")


# Close the database connection
conn.close()
