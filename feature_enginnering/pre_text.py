import json
from transformers import BertTokenizer, BertModel
import torch
import feature_db_utils

# Initialize BERT model and tokenizer
tokenizer = BertTokenizer.from_pretrained('bert-base-uncased')
model = BertModel.from_pretrained('bert-base-uncased')

def generate_embedding(text):
    """Generate embedding for a given text using BERT."""
    inputs = tokenizer(text, return_tensors='pt', max_length=512, truncation=True, padding='max_length')
    with torch.no_grad():
        outputs = model(**inputs)
    return outputs.last_hidden_state.mean(dim=1).squeeze().tolist()

def embed_titles():
    """Generate embeddings for paper titles and update the database."""
    papers = feature_db_utils.get_papers_without_title_embedding()
    for doi, title in papers:
        embedding = generate_embedding(title)
        feature_db_utils.insert_title_embedding(doi, embedding)

def embed_keywords():
    """Generate embeddings for keywords and update the database."""
    keywords = feature_db_utils.get_keywords_without_embedding()
    for keyword_id, keyword in keywords:
        embedding = generate_embedding(keyword)
        feature_db_utils.insert_keyword_embedding(keyword_id, embedding)

if __name__ == '__main__':
    embed_titles()
    embed_keywords()
