import sqlite3
import networkx as nx
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from nltk.stem import PorterStemmer
import pickle

# Database connection
conn = sqlite3.connect('data/project_data.db')
cursor = conn.cursor()

# Select Seed Period
seed_start_year = 2000
seed_end_year = 2010

# Extract Data for Seed Period
cursor.execute("SELECT doi, title FROM papers WHERE publication_year BETWEEN ? AND ?", (seed_start_year, seed_end_year))
seed_papers = cursor.fetchall()

conn.close()
