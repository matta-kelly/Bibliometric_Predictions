import sqlite3
import networkx as nx
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from nltk.stem import PorterStemmer
from collections import Counter
from itertools import combinations
import pickle
import matplotlib.pyplot as plt
import logging
import nltk

# Download NLTK resources if not already downloaded
#nltk.download('stopwords')
#nltk.download('punkt')

# Set up logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Reduce logging level for Matplotlib font manager
logging.getLogger('matplotlib.font_manager').setLevel(logging.WARNING)

def get_seed_papers(datatype, seed_start_year, seed_end_year, connection):
    cursor = connection.cursor()
    if datatype == 'title':
        query = "SELECT doi, title FROM papers WHERE publication_year BETWEEN ? AND ?"
    elif datatype == 'keyword':
        query = """
            SELECT p.doi, k.keyword 
            FROM papers p 
            JOIN paper_keywords pk ON p.doi = pk.paper_id 
            JOIN keywords k ON pk.keyword_id = k.id 
            WHERE p.publication_year BETWEEN ? AND ?
        """
    else:
        raise ValueError("Unsupported datatype. Please use 'title' or 'keyword'.")
    
    logger.debug(f"Executing query: {query}")
    cursor.execute(query, (seed_start_year, seed_end_year))
    results = cursor.fetchall()
    logger.debug(f"Retrieved {len(results)} records from the database.")
    return results

def process_text(text):
    stop_words = set(stopwords.words('english'))
    stemmer = PorterStemmer()
    words = word_tokenize(text.lower())
    words = [stemmer.stem(word) for word in words if word.isalpha() and word not in stop_words]
    return words

def create_seed_graph(seed_terms):
    term_freq = Counter([term for terms in seed_terms for term in terms])
    seed_graph = nx.Graph()

    for terms in seed_terms:
        for term in terms:
            if not seed_graph.has_node(term):
                seed_graph.add_node(term, frequency=term_freq[term])
        for pair in combinations(terms, 2):
            if seed_graph.has_edge(*pair):
                seed_graph[pair[0]][pair[1]]['weight'] += 1
            else:
                seed_graph.add_edge(*pair, weight=1)
    
    return seed_graph

def save_seed_graph(seed_graph, datatype):
    filename = f'novelty_analysis/{datatype}_seed_graph.pkl'
    with open(filename, 'wb') as f:
        pickle.dump(seed_graph, f)
    logger.info(f"Seed graph saved to {filename}")

def visualize_seed_graph(seed_graph):
    plt.figure(figsize=(12, 12))
    pos = nx.spring_layout(seed_graph, k=0.1)
    nx.draw(seed_graph, pos, with_labels=True, node_size=50, font_size=8, font_weight='bold')
    plt.title("Seed Graph Visualization")
    plt.show()

def main():
    # Configuration
    datatype = 'keyword'  # Change to 'title' to process titles
    seed_start_year = 2000
    seed_end_year = 2010

    # Database connection
    logger.info("Database connection established.")
    conn = sqlite3.connect('data/project_data.db')
    
    # Get seed papers
    seed_papers = get_seed_papers(datatype, seed_start_year, seed_end_year, conn)
    logger.info("Database connection closed.")
    conn.close()
    
    # Log sample of seed papers
    for i, (doi, text) in enumerate(seed_papers[:5]):
        logger.debug(f"Sample seed paper {i}: DOI={doi}, Text={text}")
    
    # Process terms
    seed_terms = [process_text(text) for _, text in seed_papers]
    
    # Log processed terms
    for i, terms in enumerate(seed_terms[:5]):
        logger.debug(f"Processed terms for sample paper {i}: {terms}")
    
    # Create seed graph
    seed_graph = create_seed_graph(seed_terms)
    
    # Log nodes and edges of the seed graph
    logger.debug(f"Number of nodes in seed graph: {seed_graph.number_of_nodes()}")
    logger.debug(f"Number of edges in seed graph: {seed_graph.number_of_edges()}")
    
    # Save seed graph
    save_seed_graph(seed_graph, datatype)
    
    # Visualize seed graph
    if seed_graph.number_of_nodes() > 0 and seed_graph.number_of_edges() > 0:
        visualize_seed_graph(seed_graph)
    else:
        logger.warning("Seed graph is empty. Visualization skipped.")

if __name__ == "__main__":
    main()
