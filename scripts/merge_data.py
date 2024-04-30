import pandas as pd

# File paths
ce_file = "data/WOS_data/CSV/ce-500.csv"
de_file = "data/WOS_data/CSV/de-500.csv"
se_file = "data/WOS_data/CSV/se-500.csv"
merged_file = "data/WOS_data/CSV/merged.csv"

# Read CSV files
ce_df = pd.read_csv(ce_file)
de_df = pd.read_csv(de_file)
se_df = pd.read_csv(se_file)

# Concatenate CSV files
merged_df = pd.concat([ce_df, de_df, se_df], ignore_index=True)

# Count total number of papers
total_papers = len(merged_df)

# Count NaN values before dropping
num_nan_before = merged_df['DOI'].isna().sum()

# Drop NaN values
merged_df.dropna(subset=['DOI'], inplace=True)

# Count NaN values after dropping
num_nan_after = merged_df['DOI'].isna().sum()

# Check for and drop duplicate DOIs in the merged dataset
duplicate_dois = merged_df[merged_df.duplicated(subset=['DOI'], keep=False)]
num_duplicates = len(duplicate_dois)

if num_duplicates > 0:
    print("\nDuplicate DOIs found in the merged dataset:")
    print(f"Number of duplicate DOIs: {num_duplicates}")
    print(duplicate_dois[['DOI']])
    # Drop duplicates while keeping the first occurrence
    merged_df.drop_duplicates(subset=['DOI'], keep='first', inplace=True)

# Save merged CSV file
merged_df.to_csv(merged_file, index=False)

# Count number of papers saved to the merged CSV
num_papers_saved = len(merged_df)
print(f"\nNumber of papers saved to the merged CSV: {num_papers_saved}")

print(f"Total number of papers: {total_papers}")
print(f"Number of NaN values before dropping: {num_nan_before}")
print(f"Number of NaN values after dropping: {num_nan_after}")