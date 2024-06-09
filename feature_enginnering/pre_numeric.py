import feature_db_utils

def normalize_data():
    """Normalize numerical fields in the database."""
    fields_to_normalize = [
        ('Authors', 'paperCount'),
        ('Authors', 'citationCount'),
        ('Authors', 'hIndex'),
        ('Papers', 'citation_count'),
        ('Papers', 'reference_count'),
        ('Papers', 'influential_citation_count')
    ]

    for table, field in fields_to_normalize:
        feature_db_utils.normalize_field(table, field)

def update_paper_ages():
    """Update the age of papers."""
    feature_db_utils.update_paper_age()

if __name__ == '__main__':
    update_paper_ages()
    normalize_data()