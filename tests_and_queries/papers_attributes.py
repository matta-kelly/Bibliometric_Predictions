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

# Query to find the date range of papers
date_range_query = "SELECT MIN(publication_year), MAX(publication_year) FROM papers"
date_range = execute_query(date_range_query)
print(f"Date range of papers: {date_range[0][0]} to {date_range[0][1]}")

# Close the database connection
conn.close()
