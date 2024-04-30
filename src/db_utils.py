import api_utils
import db_utils
import sqlite3

def main():
    connection = sqlite3.connect('path_to_your_database.db')
    dois = get_all_dois_from_your_dataset(connection)  # Define this function to retrieve all DOIs you want to process

    for doi in dois:
        document = api_utils.fetch_citation_data(doi)
        if document:
            citation_dois = api_utils.extract_citation_dois(document)
            for cited_doi in citation_dois:
                if db_utils.is_doi_in_dataset(cited_doi, connection):
                    db_utils.insert_citation(doi, cited_doi, connection)

    connection.close()

if __name__ == "__main__":
    main()