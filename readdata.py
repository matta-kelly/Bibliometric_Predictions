import pandas as pd

def read_and_preprocess(file_path):
    # Specify data types for each column if known (optional)
    dtype_dict = {
        'Publication Year': 'int',
        # Add other columns and their types here
    }
    
    # Specify the columns you want to use (optional)
    use_columns = ['Author Full Names', 'Article Title', 'Source Title', 'Year', 'Cited References']

    # Read the CSV file into a pandas DataFrame
    df = pd.read_csv(file_path, sep='\t', encoding='utf-8')  # Assuming tab-delimited, adjust if necessary

<<<<<<< HEAD
=======



>>>>>>> ea3a227e9a1c102c701bcb49d1cbed333c445b46
    # Preprocess individual columns as needed, here are some examples:

    # Preprocessing 'Authors' column (splitting string into a list)
    df['Authors'] = df['Authors'].apply(lambda x: x.split('; ') if pd.notnull(x) else [])

    # Preprocessing 'Publication Year' (convert to integer)
    df['Publication Year'] = pd.to_numeric(df['Publication Year'], errors='coerce')

    # Further preprocessing steps can be added here for each column as required
    # ...

    return df

# Usage
<<<<<<< HEAD
file_path = 'C:/Users/matta/.vscode/BibliometricNetwork/data'  
=======
file_path = 'C:/Users/matta/.vscode/BibliometricNetwork/data'  # Replace with your actual file path
>>>>>>> ea3a227e9a1c102c701bcb49d1cbed333c445b46
publications_df = read_and_preprocess(file_path)